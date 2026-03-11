package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/s4nj1th/free-fs/config"
	pb "github.com/s4nj1th/free-fs/proto"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedFreeFSServer
	nodeID        int
	isMaster      bool
	coordinator   int
	mu            sync.Mutex
	fileMetadata  map[string][]string // filename -> chunk handles
	chunkLocs     map[string][]int    // chunk handle -> node IDs
	lastHeartbeat map[int]time.Time
	chunkDir      string
	electionMu    sync.Mutex
}

func main() {
	idStr := os.Getenv("NODE_ID")
	id, _ := strconv.Atoi(idStr)
	
	s := &Server{
		nodeID:        id,
		isMaster:      id == 4, // 4 is explicitly initial master
		coordinator:   4,
		fileMetadata:  make(map[string][]string),
		chunkLocs:     make(map[string][]int),
		lastHeartbeat: make(map[int]time.Time),
		chunkDir:      "/data",
	}

	for n := 1; n <= 4; n++ {
		s.lastHeartbeat[n] = time.Now()
	}

	os.MkdirAll(s.chunkDir, 0755)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterFreeFSServer(grpcServer, s)

	go s.heartbeatLoop()
	go s.masterCheckLoop()

	log.Printf("Node %d starting up! Initial Master? %t", s.nodeID, s.isMaster)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *Server) startElection() {
	s.electionMu.Lock()
	defer s.electionMu.Unlock()
	
	s.mu.Lock()
	s.coordinator = -1
	s.mu.Unlock()

	log.Printf("[Node %d] Starting master election!", s.nodeID)
	higher := false

	for id, addr := range config.Nodes {
		if id > s.nodeID {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err == nil {
				c := pb.NewFreeFSClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				resp, err := c.Election(ctx, &pb.ElectionMessage{SenderId: int32(s.nodeID)})
				cancel()
				conn.Close()
				if err == nil && resp.Ok {
					higher = true
				}
			}
		}
	}

	if !higher {
		log.Printf("[Node %d] I Won! Becoming Master.", s.nodeID)
		s.mu.Lock()
		s.isMaster = true
		s.coordinator = s.nodeID
		s.fileMetadata = make(map[string][]string)
		s.chunkLocs = make(map[string][]int)
		for n := 1; n <= 4; n++ {
			s.lastHeartbeat[n] = time.Now()
		}
		s.mu.Unlock()

		for id, addr := range config.Nodes {
			if id != s.nodeID {
				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err == nil {
					c := pb.NewFreeFSClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					c.Coordinator(ctx, &pb.CoordinatorMessage{SenderId: int32(s.nodeID)})
					cancel()
					conn.Close()
				}
			}
		}
	}
}

func (s *Server) Election(ctx context.Context, req *pb.ElectionMessage) (*pb.ElectionAnswer, error) {
	if req.SenderId < int32(s.nodeID) {
		go s.startElection()
		return &pb.ElectionAnswer{Ok: true}, nil
	}
	return &pb.ElectionAnswer{Ok: false}, nil
}

func (s *Server) Coordinator(ctx context.Context, req *pb.CoordinatorMessage) (*pb.CoordinatorAnswer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.coordinator = int(req.SenderId)
	s.isMaster = false
	log.Printf("[Node %d] Node %d is the new Master.", s.nodeID, req.SenderId)
	return &pb.CoordinatorAnswer{Ok: true}, nil
}

func (s *Server) heartbeatLoop() {
	for {
		time.Sleep(config.HeartbeatInterval)
		s.mu.Lock()
		coord := s.coordinator
		isM := s.isMaster
		s.mu.Unlock()

		if isM || coord == -1 {
			continue
		}

		addr := config.Nodes[coord]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("[Node %d] Master %d unreachable: %v", s.nodeID, coord, err)
			go s.startElection()
			continue
		}

		c := pb.NewFreeFSClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = c.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: int32(s.nodeID)})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("[Node %d] Heartbeat failed: %v", s.nodeID, err)
			go s.startElection()
		}
	}
}

func (s *Server) masterCheckLoop() {
	for {
		time.Sleep(config.HeartbeatInterval)
		s.mu.Lock()
		if s.isMaster {
			for id, t := range s.lastHeartbeat {
				if id != s.nodeID && time.Since(t) > config.HeartbeatTimeout {
					log.Printf("[Master] Node %d is DEAD (no heartbeat for 10s)", id)
					delete(s.lastHeartbeat, id)
				}
			}
		}
		s.mu.Unlock()
	}
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isMaster {
		return nil, fmt.Errorf("not master")
	}
	s.lastHeartbeat[int(req.NodeId)] = time.Now()
	s.lastHeartbeat[s.nodeID] = time.Now()
	return &pb.HeartbeatResponse{Success: true}, nil
}

func (s *Server) PutFile(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isMaster {
		return nil, fmt.Errorf("not master")
	}

	alive := s.getAliveNodes()
	if len(alive) == 0 {
		return nil, fmt.Errorf("no alive nodes")
	}

	var assigns []*pb.ChunkAssignment
	var handles []string

	for i := 0; i < int(req.NumChunks); i++ {
		handle := fmt.Sprintf("%s_chunk%d", req.Filename, i)
		handles = append(handles, handle)
		
		var ips []string
		for _, node := range alive {
			ips = append(ips, config.Nodes[node])
			s.chunkLocs[handle] = append(s.chunkLocs[handle], node)
		}
		assigns = append(assigns, &pb.ChunkAssignment{
			ChunkHandle: handle,
			ChunkserverIps: ips,
		})
	}

	s.fileMetadata[req.Filename] = handles
	log.Printf("[Master] Assigned %d chunks for %s to nodes %v", req.NumChunks, req.Filename, alive)
	return &pb.PutResponse{Assignments: assigns, Success: true}, nil
}

func (s *Server) ConfirmPut(ctx context.Context, req *pb.ConfirmPutRequest) (*pb.ConfirmPutResponse, error) {
	return &pb.ConfirmPutResponse{Success: true}, nil
}

func (s *Server) GetFile(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isMaster {
		return nil, fmt.Errorf("not master")
	}

	handles, ok := s.fileMetadata[req.Filename]
	if !ok {
		return &pb.GetResponse{Success: false, Error: "file not found"}, nil
	}

	var assigns []*pb.ChunkAssignment
	for _, handle := range handles {
		var ips []string
		for _, node := range s.chunkLocs[handle] {
			ips = append(ips, config.Nodes[node])
		}
		assigns = append(assigns, &pb.ChunkAssignment{
			ChunkHandle: handle,
			ChunkserverIps: ips,
		})
	}
	return &pb.GetResponse{Chunks: assigns, Success: true}, nil
}

func (s *Server) ListFiles(ctx context.Context, req *pb.LsRequest) (*pb.LsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isMaster {
		return nil, fmt.Errorf("not master")
	}
	var f []string
	for name := range s.fileMetadata {
		f = append(f, name)
	}
	return &pb.LsResponse{Filenames: f}, nil
}

func (s *Server) StatFile(ctx context.Context, req *pb.StatRequest) (*pb.StatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isMaster {
		return nil, fmt.Errorf("not master")
	}
	handles, ok := s.fileMetadata[req.Filename]
	if !ok {
		return &pb.StatResponse{Success: false, Error: "not found"}, nil
	}
	return &pb.StatResponse{NumChunks: int32(len(handles)), Success: true}, nil
}

func (s *Server) getAliveNodes() []int {
	var alive []int
	alive = append(alive, s.nodeID)
	now := time.Now()
	for id, t := range s.lastHeartbeat {
		if id != s.nodeID && now.Sub(t) <= config.HeartbeatTimeout {
			alive = append(alive, id)
		}
	}
	if len(alive) > config.ReplicationFactor {
		return alive[:config.ReplicationFactor]
	}
	return alive
}

func (s *Server) WriteChunk(ctx context.Context, req *pb.WriteChunkRequest) (*pb.WriteChunkResponse, error) {
	err := os.WriteFile(filepath.Join(s.chunkDir, req.ChunkHandle), req.Data, 0644)
	if err != nil {
		return &pb.WriteChunkResponse{Success: false}, err
	}
	log.Printf("[Chunkserver %d] Wrote chunk %s", s.nodeID, req.ChunkHandle)
	return &pb.WriteChunkResponse{Success: true}, nil
}

func (s *Server) ReadChunk(ctx context.Context, req *pb.ReadChunkRequest) (*pb.ReadChunkResponse, error) {
	data, err := os.ReadFile(filepath.Join(s.chunkDir, req.ChunkHandle))
	if err != nil {
		return &pb.ReadChunkResponse{Success: false}, err
	}
	return &pb.ReadChunkResponse{Data: data, Success: true}, nil
}
