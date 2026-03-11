package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/s4nj1th/free-fs/config"
	pb "github.com/s4nj1th/free-fs/proto"
	"google.golang.org/grpc"
)

func getMaster() pb.FreeFSClient {
	for id := 4; id >= 1; id-- {
		conn, err := grpc.Dial(config.Nodes[id], grpc.WithInsecure())
		if err == nil {
			c := pb.NewFreeFSClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := c.ListFiles(ctx, &pb.LsRequest{})
			cancel()
			if err == nil {
				return c
			}
			conn.Close()
		}
	}
	log.Fatalf("No master found!")
	return nil
}

func put(local, remote string) {
	file, err := os.Open(local)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	info, _ := file.Stat()
	numChunks := int32((info.Size() + int64(config.ChunkSize) - 1) / int64(config.ChunkSize))
	if numChunks == 0 {
		numChunks = 1
	}

	master := getMaster()
	resp, err := master.PutFile(context.Background(), &pb.PutRequest{
		Filename:  remote,
		NumChunks: numChunks,
	})
	if err != nil || !resp.Success {
		log.Fatalf("PutFile failed: %v", err)
	}

	buf := make([]byte, config.ChunkSize)
	for i, chunkAssign := range resp.Assignments {
		n, err := file.Read(buf)
		if n == 0 || (err != nil && err != io.EOF) {
			break
		}
		data := buf[:n]

		for _, ip := range chunkAssign.ChunkserverIps {
			conn, _ := grpc.Dial(ip, grpc.WithInsecure())
			c := pb.NewFreeFSClient(conn)
			c.WriteChunk(context.Background(), &pb.WriteChunkRequest{
				ChunkHandle: chunkAssign.ChunkHandle,
				Data:        data,
			})
			conn.Close()
			fmt.Printf("Wrote chunk %d to %s\n", i, ip)
		}
	}
	master.ConfirmPut(context.Background(), &pb.ConfirmPutRequest{Filename: remote})
	fmt.Println("Put success!")
}

func get(remote, local string) {
	master := getMaster()
	resp, err := master.GetFile(context.Background(), &pb.GetRequest{Filename: remote})
	if err != nil || !resp.Success {
		log.Fatalf("GetFile failed")
	}

	file, _ := os.Create(local)
	defer file.Close()

	for _, chunk := range resp.Chunks {
		success := false
		for _, ip := range chunk.ChunkserverIps {
			conn, _ := grpc.Dial(ip, grpc.WithInsecure())
			c := pb.NewFreeFSClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			r, err := c.ReadChunk(ctx, &pb.ReadChunkRequest{ChunkHandle: chunk.ChunkHandle})
			cancel()
			conn.Close()
			if err == nil && r.Success {
				file.Write(r.Data)
				success = true
				fmt.Printf("Read %s from %s\n", chunk.ChunkHandle, ip)
				break
			}
		}
		if !success {
			log.Fatalf("Failed to read chunk %s", chunk.ChunkHandle)
		}
	}
	fmt.Println("Get success!")
}

func ls() {
	master := getMaster()
	resp, _ := master.ListFiles(context.Background(), &pb.LsRequest{})
	for _, f := range resp.Filenames {
		fmt.Println(f)
	}
}

func stat(file string) {
	master := getMaster()
	resp, _ := master.StatFile(context.Background(), &pb.StatRequest{Filename: file})
	if resp.Success {
		fmt.Printf("File: %s | Chunks: %d\n", file, resp.NumChunks)
	} else {
		fmt.Println("File not found")
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Commands: put <local> <remote>, get <remote> <local>, ls, stat <file>")
		return
	}
	cmd := os.Args[1]
	switch cmd {
	case "put":
		put(os.Args[2], os.Args[3])
	case "get":
		get(os.Args[2], os.Args[3])
	case "ls":
		ls()
	case "stat":
		stat(os.Args[2])
	default:
		fmt.Println("Unknown command")
	}
}
