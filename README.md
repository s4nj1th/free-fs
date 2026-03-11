# FREE-FS (Simplified GFS Clone)

A lightweight, from-scratch implementation of the Google File System architecture written in Go, containerized with Docker.

## Architecture

This project simulates a distributed file system with 4 generic nodes (1 Master, 3 Chunkservers) operating on a dedicated Docker bridge network with static IP addresses.

### Core Features:
- **Unified Node Binary**: Every node contains both Master and Chunkserver logic.
- **Fixed 1MB Chunks**: Files are split into exactly 1MB chunks on the client side.
- **Client-Side Streaming**: Clients retrieve chunk locations from the Master and stream data directly to Chunkservers in parallel.
- **3x Replication**: By default, each chunk is replicated across the 3 chunkservers.
- **Bully Algorithm Election**: If the Master (default Node 4) fails, the remaining nodes hold an election. The node with the highest ID becomes the new Master.
- **Heartbeat Failure Detection**: Chunkservers constantly heartbeat the Master (every 2s). If a node goes silent for 10s, the Master marks it as DEAD.
- **No Persistence**: As per requirements, Master metadata is kept entirely in-memory and lost upon election/restart to demonstrate pure state reconstruction operations.

## Quick Start (Demo)

We provide a `demo.sh` script to automatically spin up the cluster, execute read/write operations, and demonstrate the failure algorithms.

```bash
# Ensure Docker daemon is running
chmod +x demo.sh
./demo.sh
```

### Manual Usage

You can build and start the cluster manually:
```bash
docker compose build
docker compose up -d
```

Enter the client container to use the CLI:
```bash
docker exec -it free-fs-client-1 /bin/sh

# Commands:
/client put /tmp/localfile.txt /remote/path.txt
/client get /remote/path.txt /tmp/downloaded.txt
/client ls
/client stat /remote/path.txt
```

## Structure
- `cmd/node/main.go` - The unified Master/Chunkserver node implementation.
- `cmd/client/main.go` - The client CLI.
- `proto/free_fs.proto` - The shared gRPC definitions for all operations.
- `config/config.go` - Fixed node configurations and parameters.
- `docker-compose.yml` - Defines the 5 containers on the `free_fs_net` bridge network.
