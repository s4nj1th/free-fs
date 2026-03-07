#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

echo ""
echo -e "${CYAN}${BOLD}"
echo "  ██████╗ ███████╗███████╗"
echo " ██╔════╝ ██╔════╝██╔════╝"
echo " ██║  ███╗█████╗  ███████╗"
echo " ██║   ██║██╔══╝  ╚════██║"
echo " ╚██████╔╝██║     ███████║"
echo "  ╚═════╝ ╚═╝     ╚══════╝  Build Script"
echo -e "${NC}"

cd "$(dirname "$0")/.."

echo -e "${CYAN}[1/4] Installing Go protoc plugins...${NC}"
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
echo -e "${CYAN}[2/4] Generating gRPC stubs (Go)...${NC}"
mkdir -p proto/github.com/free-fs/proto

protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/free_fs.proto

echo -e "${CYAN}[3/4] Tidying Go modules...${NC}"
go mod tidy
echo -e "${CYAN}[4/4] Building binaries...${NC}"
mkdir -p bin

CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/free-fs-master ./master/
CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/free-fs-chunk ./chunkserver/

echo -e "${GREEN}${BOLD}✓ Build complete!${NC}"
echo ""
echo -e "  ${CYAN}bin/free-fs-master${NC}  — Run the master server"
echo -e "  ${CYAN}bin/free-fs-chunk${NC}   — Run a chunk server"
echo ""
