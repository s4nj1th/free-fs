#!/bin/bash
# FREE-FS Quick Start Script
set -e
BOLD='\033[1m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
banner() {
echo -e "${CYAN}${BOLD}"
cat << 'EOF'
 ██████╗ ███████╗███████╗  Replica
██╔════╝ ██╔════╝██╔════╝  Distributed File System
██║  ███╗█████╗  ███████╗
██║   ██║██╔══╝  ╚════██║  github.com/s4nj1th/free-fs
╚██████╔╝██║     ███████║
 ╚═════╝ ╚═╝     ╚══════╝
EOF
echo -e "${NC}"
}
usage() {
    banner
    echo -e "  ${BOLD}Usage:${NC} $0 <command>"
    echo ""
    echo -e "  ${CYAN}up${NC}          Start full cluster (master + 3 chunk servers)"
    echo -e "  ${CYAN}down${NC}        Stop all containers"
    echo -e "  ${CYAN}status${NC}      Show cluster status"
    echo -e "  ${CYAN}demo${NC}        Run interactive demo"
    echo -e "  ${CYAN}shell${NC}       Open interactive FREE-FS shell"
    echo -e "  ${CYAN}logs${NC}        Show logs"
    echo -e "  ${CYAN}scale N${NC}     Scale to N chunk servers"
    echo -e "  ${CYAN}put L R${NC}     Upload local file L to remote path R"
    echo -e "  ${CYAN}get R L${NC}     Download remote R to local L"
    echo -e "  ${CYAN}ls [PATH]${NC}   List files"
    echo -e "  ${CYAN}build${NC}       Build Docker images"
    echo ""
}
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}✗ Docker not found. Install Docker first.${NC}"
        exit 1
    fi
    if ! docker compose version &> /dev/null 2>&1; then
        if ! docker-compose version &> /dev/null 2>&1; then
            echo -e "${RED}✗ docker compose not found.${NC}"
            exit 1
        fi
        DC="docker-compose"
    else
        DC="docker compose"
    fi
}
run_cli() {
    check_docker
    $DC -f "$SCRIPT_DIR/docker/docker-compose.yml" run --rm \
        -e FREE_FS_MASTER=master:50051 \
        cli "$@"
}
case "${1:-help}" in
    up|start)
        banner
        check_docker
        echo -e "${CYAN}Starting FREE-FS cluster...${NC}"
        $DC -f "$SCRIPT_DIR/docker/docker-compose.yml" up -d master chunk1 chunk2 chunk3
        echo ""
        echo -e "${GREEN}${BOLD}✓ FREE-FS cluster is up!${NC}"
        echo ""
        echo -e "  Master:  ${CYAN}localhost:50051${NC}"
        echo -e "  Chunk1:  ${CYAN}localhost:50052${NC}"
        echo -e "  Chunk2:  ${CYAN}localhost:50053${NC}"
        echo -e "  Chunk3:  ${CYAN}localhost:50054${NC}"
        echo ""
        echo -e "  Run ${YELLOW}$0 status${NC} to check the cluster"
        echo -e "  Run ${YELLOW}$0 demo${NC}   to see a demo"
        echo -e "  Run ${YELLOW}$0 shell${NC}  for interactive CLI"
        echo ""
        ;;
    down|stop)
        check_docker
        echo -e "${CYAN}Stopping FREE-FS cluster...${NC}"
        $DC -f "$SCRIPT_DIR/docker/docker-compose.yml" down
        echo -e "${GREEN}✓ Stopped.${NC}"
        ;;
    status)
        run_cli status
        ;;
    demo)
        run_cli demo
        ;;
    shell)
        run_cli shell
        ;;
    logs)
        check_docker
        $DC -f "$SCRIPT_DIR/docker/docker-compose.yml" logs -f "${2:-}"
        ;;
    scale)
        N="${2:-3}"
        check_docker
        echo -e "${CYAN}Scaling to ${N} chunk servers...${NC}"
        $DC -f "$SCRIPT_DIR/docker/docker-compose.yml" up -d --scale chunk1=1 --scale chunk2=1 --scale chunk3="${N}"
        ;;
    put)
        run_cli put "$2" "$3"
        ;;
    get)
        run_cli get "$2" "$3"
        ;;
    ls)
        run_cli ls "${2:-/}"
        ;;
    build)
        check_docker
        banner
        echo -e "${CYAN}Building Docker images...${NC}"
        $DC -f "$SCRIPT_DIR/docker/docker-compose.yml" build
        echo -e "${GREEN}${BOLD}✓ Build complete!${NC}"
        ;;
    *)
        usage
        ;;
esac
