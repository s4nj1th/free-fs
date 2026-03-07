package main
import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	pb "github.com/free-fs/free-fs/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)
const (
	ChunkDir      = "/data/chunks"
	DefaultPort   = "50052"
	ChunkCapacity = 10 * 1024 * 1024 * 1024 
)
type ChunkServerImpl struct {
	pb.UnimplementedChunkServerServer
	mu          sync.RWMutex
	serverID    string
	masterAddr  string
	selfAddr    string
	chunkSizes  map[string]int64
	chunkVers   map[string]int64
}
func NewChunkServer(serverID, masterAddr, selfAddr string) *ChunkServerImpl {
	os.MkdirAll(ChunkDir, 0755)
	cs := &ChunkServerImpl{
		serverID:   serverID,
		masterAddr: masterAddr,
		selfAddr:   selfAddr,
		chunkSizes: make(map[string]int64),
		chunkVers:  make(map[string]int64),
	}
	cs.loadExistingChunks()
	return cs
}
func (cs *ChunkServerImpl) loadExistingChunks() {
	entries, err := os.ReadDir(ChunkDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			chunkID := e.Name()
			info, err := e.Info()
			if err == nil {
				cs.chunkSizes[chunkID] = info.Size()
				cs.chunkVers[chunkID] = 1
			}
		}
	}
	log.Printf("[CHUNK] Loaded %d existing chunks", len(cs.chunkSizes))
}
func (cs *ChunkServerImpl) chunkPath(chunkID string) string {
	return filepath.Join(ChunkDir, chunkID)
}
func (cs *ChunkServerImpl) WriteChunk(ctx context.Context, req *pb.WriteChunkRequest) (*pb.WriteChunkResponse, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	path := cs.chunkPath(req.ChunkId)
	var f *os.File
	var err error
	if req.Offset == 0 {
		f, err = os.Create(path)
	} else {
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	}
	if err != nil {
		return &pb.WriteChunkResponse{Success: false, Error: err.Error()}, nil
	}
	defer f.Close()
	if req.Offset > 0 {
		if _, err := f.Seek(req.Offset, io.SeekStart); err != nil {
			return &pb.WriteChunkResponse{Success: false, Error: err.Error()}, nil
		}
	}
	n, err := f.Write(req.Data)
	if err != nil {
		return &pb.WriteChunkResponse{Success: false, Error: err.Error()}, nil
	}
	cs.chunkSizes[req.ChunkId] = req.Offset + int64(n)
	cs.chunkVers[req.ChunkId] = req.Version
	log.Printf("[CHUNK] WriteChunk %s offset=%d bytes=%d", req.ChunkId[:8], req.Offset, n)
	return &pb.WriteChunkResponse{Success: true, BytesWritten: int64(n)}, nil
}
func (cs *ChunkServerImpl) ReadChunk(ctx context.Context, req *pb.ReadChunkRequest) (*pb.ReadChunkResponse, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	path := cs.chunkPath(req.ChunkId)
	f, err := os.Open(path)
	if err != nil {
		return &pb.ReadChunkResponse{Success: false, Error: fmt.Sprintf("chunk not found: %s", req.ChunkId)}, nil
	}
	defer f.Close()
	if req.Offset > 0 {
		if _, err := f.Seek(req.Offset, io.SeekStart); err != nil {
			return &pb.ReadChunkResponse{Success: false, Error: err.Error()}, nil
		}
	}
	var data []byte
	if req.Length > 0 {
		data = make([]byte, req.Length)
		n, err := io.ReadFull(f, data)
		if err != nil && err != io.ErrUnexpectedEOF {
			return &pb.ReadChunkResponse{Success: false, Error: err.Error()}, nil
		}
		data = data[:n]
	} else {
		data, err = io.ReadAll(f)
		if err != nil {
			return &pb.ReadChunkResponse{Success: false, Error: err.Error()}, nil
		}
	}
	log.Printf("[CHUNK] ReadChunk %s offset=%d bytes=%d", req.ChunkId[:8], req.Offset, len(data))
	return &pb.ReadChunkResponse{Success: true, Data: data}, nil
}
func (cs *ChunkServerImpl) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	path := cs.chunkPath(req.ChunkId)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return &pb.DeleteChunkResponse{Success: false, Error: err.Error()}, nil
	}
	delete(cs.chunkSizes, req.ChunkId)
	delete(cs.chunkVers, req.ChunkId)
	return &pb.DeleteChunkResponse{Success: true}, nil
}
func (cs *ChunkServerImpl) ReplicateChunk(ctx context.Context, req *pb.ReplicateChunkRequest) (*pb.ReplicateChunkResponse, error) {
	
	conn, err := grpc.Dial(req.SourceAddr, grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)))
	if err != nil {
		return &pb.ReplicateChunkResponse{Success: false, Error: err.Error()}, nil
	}
	defer conn.Close()
	src := pb.NewChunkServerClient(conn)
	rctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resp, err := src.ReadChunk(rctx, &pb.ReadChunkRequest{ChunkId: req.ChunkId})
	if err != nil || !resp.Success {
		msg := "read failed"
		if err != nil {
			msg = err.Error()
		} else {
			msg = resp.Error
		}
		return &pb.ReplicateChunkResponse{Success: false, Error: msg}, nil
	}
	path := cs.chunkPath(req.ChunkId)
	if err := os.WriteFile(path, resp.Data, 0644); err != nil {
		return &pb.ReplicateChunkResponse{Success: false, Error: err.Error()}, nil
	}
	cs.mu.Lock()
	cs.chunkSizes[req.ChunkId] = int64(len(resp.Data))
	cs.chunkVers[req.ChunkId] = 1
	cs.mu.Unlock()
	log.Printf("[CHUNK] Replicated chunk %s from %s (%d bytes)", req.ChunkId[:8], req.SourceAddr, len(resp.Data))
	return &pb.ReplicateChunkResponse{Success: true}, nil
}
func (cs *ChunkServerImpl) GetChunkInfo(ctx context.Context, req *pb.GetChunkInfoRequest) (*pb.GetChunkInfoResponse, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	size, exists := cs.chunkSizes[req.ChunkId]
	ver := cs.chunkVers[req.ChunkId]
	return &pb.GetChunkInfoResponse{
		ChunkId: req.ChunkId,
		Size:    size,
		Version: ver,
		Exists:  exists,
	}, nil
}
func (cs *ChunkServerImpl) registerWithMaster() {
	for {
		conn, err := grpc.Dial(cs.masterAddr, grpc.WithInsecure())
		if err != nil {
			log.Printf("[CHUNK] Cannot connect to master: %v, retrying...", err)
			time.Sleep(3 * time.Second)
			continue
		}
		client := pb.NewMasterClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.RegisterChunkServer(ctx, &pb.RegisterRequest{
			Address:   cs.selfAddr,
			ServerId:  cs.serverID,
			Capacity:  ChunkCapacity,
			Available: cs.getAvailable(),
		})
		cancel()
		conn.Close()
		if err != nil {
			log.Printf("[CHUNK] Registration failed: %v, retrying...", err)
			time.Sleep(3 * time.Second)
			continue
		}
		if resp.Success {
			cs.serverID = resp.ServerId
			log.Printf("[CHUNK] Registered with master as %s", cs.serverID)
			cs.reportChunksToMaster()
			go cs.heartbeatLoop()
			return
		}
	}
}
func (cs *ChunkServerImpl) reportChunksToMaster() {
	cs.mu.RLock()
	reports := []*pb.ChunkReport{}
	for cid, size := range cs.chunkSizes {
		reports = append(reports, &pb.ChunkReport{
			ChunkId: cid,
			Size:    size,
			Version: cs.chunkVers[cid],
		})
	}
	cs.mu.RUnlock()
	if len(reports) == 0 {
		return
	}
	conn, err := grpc.Dial(cs.masterAddr, grpc.WithInsecure())
	if err != nil {
		return
	}
	defer conn.Close()
	client := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client.ReportChunks(ctx, &pb.ReportChunksRequest{
		ServerId: cs.serverID,
		Chunks:   reports,
	})
	log.Printf("[CHUNK] Reported %d chunks to master", len(reports))
}
func (cs *ChunkServerImpl) heartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		conn, err := grpc.Dial(cs.masterAddr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := pb.NewMasterClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cs.mu.RLock()
		used := cs.getTotalUsed()
		chunkCount := int64(len(cs.chunkSizes))
		cs.mu.RUnlock()
		client.Heartbeat(ctx, &pb.HeartbeatRequest{
			ServerId:   cs.serverID,
			Available:  ChunkCapacity - used,
			Used:       used,
			ChunkCount: chunkCount,
			CpuUsage:   getCPUUsage(),
			MemUsage:   getMemUsage(),
		})
		cancel()
		conn.Close()
	}
}
func (cs *ChunkServerImpl) getAvailable() int64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return ChunkCapacity - cs.getTotalUsed()
}
func (cs *ChunkServerImpl) getTotalUsed() int64 {
	var total int64
	for _, s := range cs.chunkSizes {
		total += s
	}
	return total
}
func getCPUUsage() float32 {
	
	return float32(runtime.NumGoroutine()) / 100.0
}
func getMemUsage() float32 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float32(m.Alloc) / float32(m.Sys) * 100
}
func main() {
	masterAddr := os.Getenv("MASTER_ADDR")
	if masterAddr == "" {
		masterAddr = "master:50051"
	}
	port := os.Getenv("CHUNK_PORT")
	if port == "" {
		port = DefaultPort
	}
	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		hostname, _ := os.Hostname()
		serverID = strings.ReplaceAll(hostname, "-", "")[:8]
	}
	selfAddr := os.Getenv("SELF_ADDR")
	if selfAddr == "" {
		hostname, _ := os.Hostname()
		selfAddr = fmt.Sprintf("%s:%s", hostname, port)
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("[CHUNK] Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(128*1024*1024),
		grpc.MaxSendMsgSize(128*1024*1024),
	)
	cs := NewChunkServer(serverID, masterAddr, selfAddr)
	pb.RegisterChunkServerServer(grpcServer, cs)
	reflection.Register(grpcServer)
	log.Printf("╔══════════════════════════════════════╗")
	log.Printf("║   FREE-FS CHUNK SERVER  id=%-8s port=%s ║", serverID, port)
	log.Printf("╚══════════════════════════════════════╝")
	go cs.registerWithMaster()
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[CHUNK] Failed to serve: %v", err)
	}
}
