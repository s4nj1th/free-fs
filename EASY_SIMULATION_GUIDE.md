# Easy Multi-Device GFS Simulation (No Docker)

If you find Docker networking complex, the easiest way to run this GFS simulation across different devices is to run the Go binaries directly over **Tailscale**.

## Why this is easier

- **No IP Tracking**: Tailscale gives each machine a stable hostname (e.g., `macbook`, `desktop`).
- **No Port Mapping**: You don't have to worry about Docker bridge networks or port forwarding.
- **Native Performance**: You can see exactly where files are stored in your local `/tmp` or data folders.

---

## 1. Setup the Mesh Network (Tailscale)

1. Install [Tailscale](https://tailscale.com/download) on all your devices.
2. Log in with the same account.
3. Verify you can ping each other using their Tailscale names:
   ```bash
   ping macbook
   ```

## 2. Prepare the Nodes

On **all machines**, clone the repo and build the node:

```bash
go build -o node_bin cmd/node/main.go
```

## 3. Run the Cluster

You need a single connection string (`FREEFS_NODES`) that uses the Tailscale hostnames.

**Example Connection String:**

```bash
export FREEFS_NODES="1=macbook:50051,2=desktop:50051,3=laptop:50051,4=server:50051"
```

### Run on each machine:

Open a terminal on each device and run their assigned node ID:

**Machine 1 (macbook):**

```bash
NODE_ID=1 FREEFS_NODES="..." ./node_bin
```

**Machine 2 (desktop):**

```bash
NODE_ID=2 FREEFS_NODES="..." ./node_bin
```

...and so on.

---

## 4. Interaction

On any machine, build and run the client:

```bash
go build -o client_bin cmd/client/main.go

# Put a file
./client_bin put my_local_file.txt /remote_file.txt

# List files
./client_bin ls
```

## Tips for Simulation

- **Kill a Node**: Press `Ctrl+C` on a node. The other nodes will detect the "failure" and trigger an election automatically after 10 seconds.
- **Inspect Data**: Look at the `/data` directory (or whatever path you set) on each machine to see the actual raw chunks of your files.
