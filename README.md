# FREE-FS

<div align="center">
**A production-grade distributed file system — Google File System architecture, built from scratch.**

[![CI](https://github.com/s4nj1th/free-fs/actions/workflows/ci.yml/badge.svg)](https://github.com/s4nj1th/free-fs/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/s4nj1th/free-fs?color=00D4FF)](https://github.com/s4nj1th/free-fs/releases)
[![Go](https://img.shields.io/badge/Go-1.21-00ADD8?logo=go)](https://go.dev)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://python.org)
[![gRPC](https://img.shields.io/badge/gRPC-protobuf-244c5a)](https://grpc.io)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)](https://hub.docker.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

</div>

## What is this?

FREE-FS is a faithful replica of the [Google File System](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf) (GFS). It implements the core architecture: a **single master** tracking all metadata, multiple **chunk servers** storing raw 64MB chunks, and **automatic 3x replication** with failure recovery.

Built with **Go** (master + chunk servers), **gRPC** (inter-service), **Docker** (zero-config cluster), and a **Python CLI** with rich terminal animations.

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                         FREE-FS Cluster                            │
│                                                                    │
│   ┌──────────────────┐   gRPC    ┌──────────────────────────────┐  │
│   │   CLI Client     │ <──────>  │       Master Server          │  │
│   │   (Python/Rich)  │           │  • Namespace (dirs/files)    │  │
│   └──────────────────┘           │  • Chunk->Server mappings    │  │
│                                  │  • Heartbeat monitoring      │  │
│                                  │  • Re-replication on failure │  │
│                                  │  • Persistent state (JSON)   │  │
│                                  └──────────────┬───────────────┘  │
│                                                 │ Register/Beat    │
│                                   ┌─────────────┼─────────────┐    │
│                                   v             v             v    │
│                          ┌─────────────┐ ┌──────────┐ ┌──────────┐ │
│                          │  Chunk S1   │ │ Chunk S2 │ │ Chunk S3 │ │
│                          │ :50052      │ │ :50053   │ │ :50054   │ │
│                          └─────────────┘ └──────────┘ └──────────┘ │
└────────────────────────────────────────────────────────────────────┘
```

### GFS Features Implemented

| Feature                  | Detail                                                        |
| ------------------------ | ------------------------------------------------------------- |
| **Single master**        | All metadata in-memory + persisted to JSON with atomic writes |
| **64MB chunks**          | Configurable chunk size, each assigned a UUID                 |
| **3x replication**       | Writes go to primary + 2 replicas; reads from any             |
| **Heartbeat monitoring** | 10s interval, 30s timeout, dead-server detection              |
| **Auto re-replication**  | Under-replicated chunks re-replicated when a server dies      |
| **Primary election**     | Each chunk has a designated primary for write coordination    |
| **Namespace**            | Hierarchical directories, file metadata, move/rename          |
| **Fault tolerance**      | Reads fall back to replica if primary is unavailable          |

## Quick Start

### Option A — Docker (recommended)

```bash
git clone https://github.com/s4nj1th/free-fs
cd free-fs
./free-fs.sh up      # Start 1 master + 3 chunk servers
./free-fs.sh status  # Check cluster health
./free-fs.sh demo    # Animated demo of all features
./free-fs.sh shell   # Interactive CLI shell
```

### Option B — Pre-built binaries

Download from [Releases](https://github.com/s4nj1th/free-fs/releases):

```bash
# Machine 1 — Master
./free-fs-master-linux-amd64

# Machines 2-4 — Chunk Servers
MASTER_ADDR=<master-ip>:50051 SERVER_ID=chunk1 SELF_ADDR=<my-ip>:50052 \
  ./free-fs-chunk-linux-amd64

# CLI (any machine)
pip install grpcio rich protobuf grpcio-tools
FREE_FS_MASTER=<master-ip>:50051 python cli/free_fs_cli.py shell
```

### Option C — Build from source

```bash
bash docker/build.sh   # Requires Go 1.21+, protoc
```

## CLI Commands

```
free-fs put <local> <remote>     Upload a file
free-fs get <remote> <local>     Download a file
free-fs ls [path]                List directory
free-fs rm <path>                Delete file
free-fs stat <path>              File metadata
free-fs mkdir <path>             Create directory
free-fs mv <src> <dst>           Move/rename
free-fs status                   Live cluster dashboard
free-fs demo                     Interactive walkthrough
free-fs shell                    Interactive REPL
```

Set `FREE_FS_MASTER=<addr>` or pass `--master <addr>` to point at your master.

## Multi-Machine Deployment

```bash
# Machine 1 — Master (e.g. 192.168.1.10)
docker run -d -p 50051:50051 -v free_fs_master:/data \
  ghcr.io/s4nj1th/free-fs/master:latest

# Machines 2-N — Chunk Servers
docker run -d -p 50052:50052 -v free_fs_chunk:/data \
  -e MASTER_ADDR=192.168.1.10:50051 \
  -e SELF_ADDR=$(hostname -I | awk '{print $1}'):50052 \
  -e SERVER_ID=$(hostname) \
  ghcr.io/s4nj1th/free-fs/chunk:latest

# Any machine — CLI
docker run --rm -it \
  -e FREE_FS_MASTER=192.168.1.10:50051 \
  ghcr.io/s4nj1th/free-fs/cli:latest status
```

Open ports: `50051/tcp` on master, `50052/tcp` on each chunk server.

## Fault Tolerance Demo

```bash
./free-fs.sh up
./free-fs.sh put README.md /test/readme.md

# Kill a chunk server
docker stop free-fs-chunk1

# Master detects failure, triggers re-replication automatically
./free-fs.sh logs master

# File still fully accessible
./free-fs.sh get /test/readme.md /tmp/recovered.md
diff README.md /tmp/recovered.md && echo "Data intact"
```

## Environment Variables

| Variable         | Default           | Used by      |
| ---------------- | ----------------- | ------------ |
| `MASTER_PORT`    | `50051`           | Master       |
| `MASTER_ADDR`    | `master:50051`    | Chunk server |
| `CHUNK_PORT`     | `50052`           | Chunk server |
| `SERVER_ID`      | hostname          | Chunk server |
| `SELF_ADDR`      | `hostname:port`   | Chunk server |
| `FREE_FS_MASTER` | `localhost:50051` | CLI          |

## Project Structure

```
free-fs/
├── free-fs.sh                    <- Main entrypoint
├── proto/free_fs.proto           <- gRPC definitions
├── master/main.go                <- Master server
├── chunkserver/main.go           <- Chunk server
├── cli/free_fs_cli.py            <- Python CLI
├── docker/
│   ├── docker-compose.yml        <- Local cluster
│   ├── docker-compose.multi.yml  <- Multi-machine
│   └── Dockerfile.*
└── .github/workflows/            <- CI + Release automation
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)

<div align="center">
<sub>Based on the <a href="https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf">Google File System paper</a> — Ghemawat, Gobioff, Leung (SOSP 2003)</sub>
</div>
