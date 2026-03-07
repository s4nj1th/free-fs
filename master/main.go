package main
import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"github.com/google/uuid"
	pb "github.com/free-fs/free-fs/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)
const (
	ChunkSize         = 64 * 1024 * 1024 
	ReplicationFactor = 3
	HeartbeatTimeout  = 30 * time.Second
	StateFile         = "/data/master_state.json"
)
type FileMetadata struct {
	Path      string   `json:"path"`
	FileID    string   `json:"file_id"`
	Size      int64    `json:"size"`
	CreatedAt int64    `json:"created_at"`
	UpdatedAt int64    `json:"updated_at"`
	ChunkIDs  []string `json:"chunk_ids"`
	Owner     string   `json:"owner"`
}
type ChunkMetadata struct {
	ChunkID   string   `json:"chunk_id"`
	FileID    string   `json:"file_id"`
	Index     int64    `json:"index"`
	Size      int64    `json:"size"`
	Version   int64    `json:"version"`
	Locations []string `json:"locations"` 
	Primary   string   `json:"primary"`
}
type ChunkServerInfo struct {
	ServerID      string  `json:"server_id"`
	Address       string  `json:"address"`
	Capacity      int64   `json:"capacity"`
	Available     int64   `json:"available"`
	Used          int64   `json:"used"`
	ChunkCount    int64   `json:"chunk_count"`
	CPUUsage      float32 `json:"cpu_usage"`
	MemUsage      float32 `json:"mem_usage"`
	LastHeartbeat int64   `json:"last_heartbeat"`
	Alive         bool    `json:"alive"`
}
type MasterState struct {
	Files  map[string]*FileMetadata  `json:"files"`   
	Chunks map[string]*ChunkMetadata `json:"chunks"`  
	Dirs   map[string]bool           `json:"dirs"`
}
type MasterServer struct {
	pb.UnimplementedMasterServer
	mu           sync.RWMutex
	state        MasterState
	chunkServers map[string]*ChunkServerInfo 
	csMu         sync.RWMutex
}
func NewMasterServer() *MasterServer {
	ms := &MasterServer{
		chunkServers: make(map[string]*ChunkServerInfo),
		state: MasterState{
			Files:  make(map[string]*FileMetadata),
			Chunks: make(map[string]*ChunkMetadata),
			Dirs:   map[string]bool{"/": true},
		},
	}
	ms.loadState()
	go ms.heartbeatMonitor()
	go ms.periodicSave()
	return ms
}
func (s *MasterServer) loadState() {
	data, err := os.ReadFile(StateFile)
	if err != nil {
		log.Printf("[MASTER] No existing state, starting fresh")
		return
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		log.Printf("[MASTER] Failed to load state: %v", err)
		return
	}
	log.Printf("[MASTER] Loaded state: %d files, %d chunks", len(s.state.Files), len(s.state.Chunks))
}
func (s *MasterServer) saveState() {
	s.mu.RLock()
	data, err := json.MarshalIndent(s.state, "", "  ")
	s.mu.RUnlock()
	if err != nil {
		log.Printf("[MASTER] Failed to marshal state: %v", err)
		return
	}
	os.MkdirAll(filepath.Dir(StateFile), 0755)
	if err := os.WriteFile(StateFile+".tmp", data, 0644); err != nil {
		log.Printf("[MASTER] Failed to write state: %v", err)
		return
	}
	os.Rename(StateFile+".tmp", StateFile)
}
func (s *MasterServer) periodicSave() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		s.saveState()
	}
}
func (s *MasterServer) heartbeatMonitor() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		now := time.Now().Unix()
		s.csMu.Lock()
		for id, cs := range s.chunkServers {
			wasAlive := cs.Alive
			cs.Alive = (now - cs.LastHeartbeat) < int64(HeartbeatTimeout.Seconds())
			if wasAlive && !cs.Alive {
				log.Printf("[MASTER] ChunkServer %s (%s) DIED - triggering re-replication", id, cs.Address)
				go s.handleChunkServerFailure(id)
			}
		}
		s.csMu.Unlock()
	}
}
func (s *MasterServer) handleChunkServerFailure(deadServerID string) {
	s.mu.Lock()
	under := []*ChunkMetadata{}
	for _, chunk := range s.state.Chunks {
		newLocs := []string{}
		for _, loc := range chunk.Locations {
			if loc != deadServerID {
				newLocs = append(newLocs, loc)
			}
		}
		chunk.Locations = newLocs
		if chunk.Primary == deadServerID && len(newLocs) > 0 {
			chunk.Primary = newLocs[0]
		}
		if len(newLocs) < ReplicationFactor {
			under = append(under, chunk)
		}
	}
	s.mu.Unlock()
	for _, chunk := range under {
		s.scheduleReplication(chunk)
	}
}
func (s *MasterServer) scheduleReplication(chunk *ChunkMetadata) {
	if len(chunk.Locations) == 0 {
		log.Printf("[MASTER] CRITICAL: chunk %s has NO replicas - data lost!", chunk.ChunkID)
		return
	}
	needed := ReplicationFactor - len(chunk.Locations)
	targets := s.selectChunkServers(needed, chunk.Locations)
	for _, target := range targets {
		s.csMu.RLock()
		cs, ok := s.chunkServers[target]
		s.csMu.RUnlock()
		if !ok {
			continue
		}
		source := chunk.Primary
		s.csMu.RLock()
		srcCS, ok2 := s.chunkServers[source]
		s.csMu.RUnlock()
		if !ok2 {
			continue
		}
		go func(chunkID, srcAddr, tgtAddr, tgtID string) {
			conn, err := grpc.Dial(tgtAddr, grpc.WithInsecure())
			if err != nil {
				return
			}
			defer conn.Close()
			client := pb.NewChunkServerClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			resp, err := client.ReplicateChunk(ctx, &pb.ReplicateChunkRequest{
				ChunkId:    chunkID,
				SourceAddr: srcAddr,
			})
			if err == nil && resp.Success {
				s.mu.Lock()
				if c, ok := s.state.Chunks[chunkID]; ok {
					c.Locations = append(c.Locations, tgtID)
				}
				s.mu.Unlock()
				log.Printf("[MASTER] Re-replicated chunk %s to %s", chunkID, tgtAddr)
			}
		}(chunk.ChunkID, srcCS.Address, cs.Address, target)
	}
}
func (s *MasterServer) selectChunkServers(n int, exclude []string) []string {
	excludeSet := make(map[string]bool)
	for _, e := range exclude {
		excludeSet[e] = true
	}
	s.csMu.RLock()
	defer s.csMu.RUnlock()
	candidates := []*ChunkServerInfo{}
	for _, cs := range s.chunkServers {
		if cs.Alive && !excludeSet[cs.ServerID] && cs.Available > ChunkSize {
			candidates = append(candidates, cs)
		}
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	result := []string{}
	for i := 0; i < n && i < len(candidates); i++ {
		result = append(result, candidates[i].ServerID)
	}
	return result
}
func (s *MasterServer) serverIDToAddr(serverID string) string {
	s.csMu.RLock()
	defer s.csMu.RUnlock()
	if cs, ok := s.chunkServers[serverID]; ok {
		return cs.Address
	}
	return ""
}
func (s *MasterServer) RegisterChunkServer(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	s.csMu.Lock()
	defer s.csMu.Unlock()
	serverID := req.ServerId
	if serverID == "" {
		serverID = uuid.New().String()[:8]
	}
	s.chunkServers[serverID] = &ChunkServerInfo{
		ServerID:      serverID,
		Address:       req.Address,
		Capacity:      req.Capacity,
		Available:     req.Available,
		LastHeartbeat: time.Now().Unix(),
		Alive:         true,
	}
	log.Printf("[MASTER] ChunkServer registered: %s @ %s (%.1f GB free)",
		serverID, req.Address, float64(req.Available)/1e9)
	return &pb.RegisterResponse{Success: true, ServerId: serverID}, nil
}
func (s *MasterServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.csMu.Lock()
	cs, ok := s.chunkServers[req.ServerId]
	if ok {
		cs.Available     = req.Available
		cs.Used          = req.Used
		cs.ChunkCount    = req.ChunkCount
		cs.CPUUsage      = req.CpuUsage
		cs.MemUsage      = req.MemUsage
		cs.LastHeartbeat = time.Now().Unix()
		cs.Alive         = true
	}
	s.csMu.Unlock()
	return &pb.HeartbeatResponse{Alive: true}, nil
}
func (s *MasterServer) ReportChunks(ctx context.Context, req *pb.ReportChunksRequest) (*pb.ReportChunksResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cr := range req.Chunks {
		if chunk, ok := s.state.Chunks[cr.ChunkId]; ok {
			found := false
			for _, loc := range chunk.Locations {
				if loc == req.ServerId {
					found = true
					break
				}
			}
			if !found {
				chunk.Locations = append(chunk.Locations, req.ServerId)
			}
		}
	}
	return &pb.ReportChunksResponse{Success: true}, nil
}
func (s *MasterServer) OpenFile(ctx context.Context, req *pb.OpenFileRequest) (*pb.OpenFileResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := normalizePath(req.Path)
	switch req.Mode {
	case "create", "w":
		return s.createOrOpenWrite(path, req.FileSize)
	case "r":
		return s.openRead(path)
	default:
		return s.openRead(path)
	}
}
func (s *MasterServer) createOrOpenWrite(path string, fileSize int64) (*pb.OpenFileResponse, error) {
	
	parent := filepath.Dir(path)
	if _, ok := s.state.Dirs[parent]; !ok {
		return &pb.OpenFileResponse{Success: false, Error: fmt.Sprintf("directory %s does not exist", parent)}, nil
	}
	
	existing, exists := s.state.Files[path]
	if exists {
		
		for _, cid := range existing.ChunkIDs {
			delete(s.state.Chunks, cid)
		}
	}
	fileID := uuid.New().String()
	numChunks := int64(1)
	if fileSize > 0 {
		numChunks = (fileSize + ChunkSize - 1) / ChunkSize
	}
	chunks := []*pb.Chunk{}
	chunkIDs := []string{}
	for i := int64(0); i < numChunks; i++ {
		chunkID := uuid.New().String()
		servers := s.selectChunkServers(ReplicationFactor, nil)
		if len(servers) == 0 {
			return &pb.OpenFileResponse{Success: false, Error: "no available chunk servers"}, nil
		}
		primary := servers[0]
		addrs := []string{}
		for _, sid := range servers {
			addrs = append(addrs, s.serverIDToAddrLocked(sid))
		}
		s.state.Chunks[chunkID] = &ChunkMetadata{
			ChunkID:   chunkID,
			FileID:    fileID,
			Index:     i,
			Version:   1,
			Locations: servers,
			Primary:   primary,
		}
		chunkIDs = append(chunkIDs, chunkID)
		chunks = append(chunks, &pb.Chunk{
			ChunkId:   chunkID,
			Index:     i,
			Locations: addrs,
			Primary:   s.serverIDToAddrLocked(primary),
			Version:   1,
		})
	}
	now := time.Now().Unix()
	s.state.Files[path] = &FileMetadata{
		Path:      path,
		FileID:    fileID,
		Size:      fileSize,
		CreatedAt: now,
		UpdatedAt: now,
		ChunkIDs:  chunkIDs,
		Owner:     "free-fs-user",
	}
	log.Printf("[MASTER] Created file %s (%d chunks)", path, numChunks)
	return &pb.OpenFileResponse{
		Success:  true,
		FileId:   fileID,
		Path:     path,
		Chunks:   chunks,
		FileSize: fileSize,
	}, nil
}
func (s *MasterServer) openRead(path string) (*pb.OpenFileResponse, error) {
	meta, ok := s.state.Files[path]
	if !ok {
		return &pb.OpenFileResponse{Success: false, Error: fmt.Sprintf("file not found: %s", path)}, nil
	}
	chunks := []*pb.Chunk{}
	for _, cid := range meta.ChunkIDs {
		cm, ok := s.state.Chunks[cid]
		if !ok {
			continue
		}
		addrs := []string{}
		for _, sid := range cm.Locations {
			if addr := s.serverIDToAddrLocked(sid); addr != "" {
				addrs = append(addrs, addr)
			}
		}
		chunks = append(chunks, &pb.Chunk{
			ChunkId:   cid,
			Index:     cm.Index,
			Locations: addrs,
			Primary:   s.serverIDToAddrLocked(cm.Primary),
			Size:      cm.Size,
			Version:   cm.Version,
		})
	}
	return &pb.OpenFileResponse{
		Success:  true,
		FileId:   meta.FileID,
		Path:     meta.Path,
		Chunks:   chunks,
		FileSize: meta.Size,
	}, nil
}
func (s *MasterServer) serverIDToAddrLocked(serverID string) string {
	s.csMu.RLock()
	defer s.csMu.RUnlock()
	if cs, ok := s.chunkServers[serverID]; ok {
		return cs.Address
	}
	return ""
}
func (s *MasterServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := normalizePath(req.Path)
	meta, ok := s.state.Files[path]
	if !ok {
		return &pb.DeleteFileResponse{Success: false, Error: "file not found"}, nil
	}
	for _, cid := range meta.ChunkIDs {
		delete(s.state.Chunks, cid)
	}
	delete(s.state.Files, path)
	log.Printf("[MASTER] Deleted file %s", path)
	return &pb.DeleteFileResponse{Success: true}, nil
}
func (s *MasterServer) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	files := []*pb.FileInfo{}
	for path, meta := range s.state.Files {
		if req.Pattern == "" || req.Pattern == "*" || strings.Contains(path, req.Pattern) {
			files = append(files, &pb.FileInfo{
				Path:       meta.Path,
				Size:       meta.Size,
				CreatedAt:  meta.CreatedAt,
				UpdatedAt:  meta.UpdatedAt,
				ChunkCount: int64(len(meta.ChunkIDs)),
				Owner:      meta.Owner,
			})
		}
	}
	return &pb.ListFilesResponse{Files: files}, nil
}
func (s *MasterServer) GetFileInfo(ctx context.Context, req *pb.GetFileInfoRequest) (*pb.GetFileInfoResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	path := normalizePath(req.Path)
	meta, ok := s.state.Files[path]
	if !ok {
		return &pb.GetFileInfoResponse{Success: false, Error: "file not found"}, nil
	}
	return &pb.GetFileInfoResponse{
		Success: true,
		Info: &pb.FileInfo{
			Path:       meta.Path,
			Size:       meta.Size,
			CreatedAt:  meta.CreatedAt,
			UpdatedAt:  meta.UpdatedAt,
			ChunkCount: int64(len(meta.ChunkIDs)),
			Owner:      meta.Owner,
		},
	}, nil
}
func (s *MasterServer) GetChunkLocations(ctx context.Context, req *pb.ChunkLocationRequest) (*pb.ChunkLocationResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	for _, cm := range s.state.Chunks {
		if cm.FileID == req.FileId && cm.Index == req.ChunkIndex {
			addrs := []string{}
			for _, sid := range cm.Locations {
				if addr := s.serverIDToAddrLocked(sid); addr != "" {
					addrs = append(addrs, addr)
				}
			}
			return &pb.ChunkLocationResponse{
				Success: true,
				Chunk: &pb.Chunk{
					ChunkId:   cm.ChunkID,
					Index:     cm.Index,
					Locations: addrs,
					Primary:   s.serverIDToAddrLocked(cm.Primary),
					Version:   cm.Version,
				},
			}, nil
		}
	}
	return &pb.ChunkLocationResponse{Success: false, Error: "chunk not found"}, nil
}
func (s *MasterServer) CreateDirectory(ctx context.Context, req *pb.CreateDirRequest) (*pb.CreateDirResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := normalizePath(req.Path)
	parent := filepath.Dir(path)
	if _, ok := s.state.Dirs[parent]; !ok && parent != path {
		return &pb.CreateDirResponse{Success: false, Error: "parent directory does not exist"}, nil
	}
	s.state.Dirs[path] = true
	return &pb.CreateDirResponse{Success: true}, nil
}
func (s *MasterServer) ListDirectory(ctx context.Context, req *pb.ListDirRequest) (*pb.ListDirResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	path := normalizePath(req.Path)
	if _, ok := s.state.Dirs[path]; !ok {
		return &pb.ListDirResponse{Success: false, Error: "directory not found"}, nil
	}
	entries := []string{}
	
	for d := range s.state.Dirs {
		if filepath.Dir(d) == path && d != path {
			entries = append(entries, d+"/")
		}
	}
	
	for fp := range s.state.Files {
		if filepath.Dir(fp) == path {
			entries = append(entries, fp)
		}
	}
	return &pb.ListDirResponse{Success: true, Entries: entries}, nil
}
func (s *MasterServer) MoveFile(ctx context.Context, req *pb.MoveFileRequest) (*pb.MoveFileResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := normalizePath(req.Src)
	dst := normalizePath(req.Dst)
	meta, ok := s.state.Files[src]
	if !ok {
		return &pb.MoveFileResponse{Success: false, Error: "source file not found"}, nil
	}
	meta.Path = dst
	meta.UpdatedAt = time.Now().Unix()
	s.state.Files[dst] = meta
	delete(s.state.Files, src)
	return &pb.MoveFileResponse{Success: true}, nil
}
func (s *MasterServer) GetClusterStatus(ctx context.Context, req *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	s.mu.RLock()
	totalFiles := int64(len(s.state.Files))
	totalChunks := int64(len(s.state.Chunks))
	s.mu.RUnlock()
	s.csMu.RLock()
	defer s.csMu.RUnlock()
	servers := []*pb.ChunkServerStatus{}
	var totalCap, usedCap int64
	for _, cs := range s.chunkServers {
		totalCap += cs.Capacity
		usedCap += cs.Used
		servers = append(servers, &pb.ChunkServerStatus{
			ServerId:      cs.ServerID,
			Address:       cs.Address,
			Alive:         cs.Alive,
			Capacity:      cs.Capacity,
			Available:     cs.Available,
			ChunkCount:    cs.ChunkCount,
			CpuUsage:      cs.CPUUsage,
			MemUsage:      cs.MemUsage,
			LastHeartbeat: cs.LastHeartbeat,
		})
	}
	return &pb.ClusterStatusResponse{
		Servers:       servers,
		TotalFiles:    totalFiles,
		TotalChunks:   totalChunks,
		TotalCapacity: totalCap,
		UsedCapacity:  usedCap,
	}, nil
}
func normalizePath(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return filepath.Clean(p)
}
func main() {
	port := os.Getenv("MASTER_PORT")
	if port == "" {
		port = "50051"
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("[MASTER] Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(128*1024*1024),
		grpc.MaxSendMsgSize(128*1024*1024),
	)
	master := NewMasterServer()
	pb.RegisterMasterServer(grpcServer, master)
	reflection.Register(grpcServer)
	log.Printf("╔══════════════════════════════════════╗")
	log.Printf("║     FREE-FS MASTER SERVER  port=%s     ║", port)
	log.Printf("╚══════════════════════════════════════╝")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[MASTER] Failed to serve: %v", err)
	}
}
