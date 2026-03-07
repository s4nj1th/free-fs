# Contributing to FREE-FS

Thanks for your interest! Here's how to get set up.

## Development Setup

### Prerequisites

- Go 1.21+
- Python 3.11+
- Docker + Docker Compose
- `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc`

### Install protoc plugins

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Build

```bash
bash docker/build.sh
```

### Run locally (without Docker)

```bash
# Terminal 1 — Master
MASTER_PORT=50051 ./bin/free-fs-master
# Terminal 2, 3, 4 — Chunk Servers
MASTER_ADDR=localhost:50051 SERVER_ID=c1 SELF_ADDR=localhost:50052 CHUNK_PORT=50052 ./bin/free-fs-chunk
MASTER_ADDR=localhost:50051 SERVER_ID=c2 SELF_ADDR=localhost:50053 CHUNK_PORT=50053 ./bin/free-fs-chunk
MASTER_ADDR=localhost:50051 SERVER_ID=c3 SELF_ADDR=localhost:50054 CHUNK_PORT=50054 ./bin/free-fs-chunk
# Terminal 5 — CLI
cd cli
pip install -r requirements.txt
python -m grpc_tools.protoc -I../proto --python_out=../proto --grpc_python_out=../proto ../proto/free_fs.proto
FREE_FS_MASTER=localhost:50051 python free_fs_cli.py shell
```

### Run with Docker

```bash
./free-fs.sh up
./free-fs.sh demo
```

## Project Structure

```
free-fs/
├── proto/free_fs.proto     ← All gRPC service + message definitions
├── master/main.go          ← Master server (single process, all metadata)
├── chunkserver/main.go     ← Chunk server (data storage, heartbeats)
├── cli/free_fs_cli.py      ← Python CLI with Rich animations
├── docker/                 ← Dockerfiles + docker-compose
└── .github/                ← CI workflows, issue templates
```

## Modifying the Proto

If you change `proto/free_fs.proto`, regenerate stubs:

```bash
# Go stubs (used by master + chunk server)
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/free_fs.proto
# Python stubs (used by CLI — auto-generated in Dockerfile.cli)
python -m grpc_tools.protoc \
  -I./proto \
  --python_out=./proto \
  --grpc_python_out=./proto \
  ./proto/free_fs.proto
```

Proto changes must be **backwards-compatible** unless it's a major version bump.

## Commit Style

We use conventional commits:

- `feat:` new feature
- `fix:` bug fix
- `refactor:` code change that doesn't add/fix
- `docs:` documentation only
- `ci:` CI/CD changes
- `chore:` maintenance

## Releasing

Releases are automated via GitHub Actions. To cut a release:

```bash
git tag v1.2.3
git push origin v1.2.3
```

This triggers the release workflow which:

1. Builds binaries for all platforms
2. Pushes Docker images to GHCR
3. Creates a GitHub Release with artifacts + checksums
