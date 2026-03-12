# Running FREE-FS Containers across Multiple Machines

This guide explains how to connect FREE-FS nodes running in Docker containers across different physical machines.

## Strategy A: Port Mapping & Host IPs (Simplest for LAN)

This is the easiest method if all machines are on the same local network and can ping each other's IP addresses.

### 1. Prerequisites

- All machines on the same Wi-Fi/Ethernet.
- Docker and Docker Compose installed on each.
- Know the local IP of each machine (e.g., `192.168.1.10`).

### 2. Configuration (`FREEFS_NODES`)

Decide on a port for each node (e.g., `50051`). Your connection string will look like this:

```bash
export FREEFS_NODES="1=192.168.1.101:50051,2=192.168.1.102:50051,3=192.168.1.103:50051,4=192.168.1.104:50051"
```

### 3. Machine-Specific Setup

On **each machine**, create a simplified `docker-compose.yml`:

```yaml
version: "3.8"
services:
  free-fs-node:
    build:
      context: .
      dockerfile: docker/Dockerfile.node
    environment:
      - NODE_ID=${NODE_ID}
      - FREEFS_NODES=${FREEFS_NODES}
    ports:
      - "50051:50051"
```

**Run on Machine 1:**

```bash
NODE_ID=1 FREEFS_NODES="..." docker-compose up
```

**Run on Machine 2:**

```bash
NODE_ID=2 FREEFS_NODES="..." docker-compose up
```

_(and so on)_

---

## Strategy B: Docker Swarm (Overlay Network)

This is the "standard" way for production-like multi-node Docker setups. Containers talk to each other using service names over a virtual network.

### 1. Initialize the Swarm

On the **Master machine**:

```bash
docker swarm init --advertise-addr <MASTER_IP>
```

Copy the `docker swarm join` command shown and run it on all other machines.

### 2. Create the Overlay Network

On the Master:

```bash
docker network create --driver overlay free_fs_net
```

### 3. Deploy as a Stack

Create a `docker-stack.yml`:

```yaml
version: "3.8"
services:
  node:
    image: your-repo/free-fs-node
    networks:
      - free_fs_net
    deploy:
      replicas: 4
      placement:
        max_replicas_per_node: 1
    environment:
      - FREEFS_NODES=1=node_1:50051,2=node_2:50051...
networks:
  free_fs_net:
    external: true
```

_Note: This requires a bit more logic in `config.go` or DNS discovery to map `node_1` to a specific container._

---

## Strategy C: Tailscale (Easiest for Remote/Cloud)

If your machines are in different locations (e.g., home and office), use Tailscale to create a secure mesh network.

1. Install [Tailscale](https://tailscale.com/) on all machines.
2. Get the Tailscale IP for each machine (e.g., `100.x.y.z`).
3. Use Strategy A, but replace local IPs with Tailscale IPs.
4. No firewall configuration needed!

---

## Troubleshooting

- **Firewalls:** Ensure port `50051` is open on all machines.
- **Port Conflicts:** If running multiple containers on one machine for testing, map them to different host ports (e.g., `50052:50051`) and update `FREEFS_NODES` accordingly.
- **Docker Network:** When using `docker-compose`, remember that `bridge` mode isolates containers. Port mapping (`-p`) is required to bridge the host network.
