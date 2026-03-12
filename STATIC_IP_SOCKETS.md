# Static IP & Socket Programming Guide

This guide explains how to run FREE-FS using manual static IP addresses on your local network instead of Docker or Tailscale.

## 1. The Socket Foundation
Even though the code uses gRPC, it is entirely built on top of standard Go **Socket Programming**.

- **Server-side**: The code uses `net.Listen("tcp", ":50051")` to open a TCP socket.
- **Client-side**: The code uses `grpc.Dial(address)` which internally performs a `net.Dial` to establish a socket connection.

You are seeing "Software-Defined Storage" (like GFS) working directly over raw network sockets.

---

## 2. Setting Up Static IPs (LAN)
To ensure your devices always have the same address, you should set Static IPs on your machines.

### On macOS:
1. System Settings -> Network -> Wi-Fi (or Ethernet) -> Details.
2. TCP/IP -> Configure IPv4 -> **Manually**.
3. Set IP Address (e.g., `192.168.1.50`), Subnet Mask (`255.255.255.0`), and Router IP.

### On Windows:
1. Settings -> Network & Internet -> Ethernet/Wi-Fi -> Edit IP settings.
2. Change to **Manual**, turn on IPv4, and enter your details.

---

## 3. Configuring FREE-FS
Once your machines have static IPs, you can hardcode them so you never have to set environment variables again.

### Option A: Edit `config/config.go`
Modify the `Nodes` map in [config.go](file:///Users/s4n/dev/free-fs/config/config.go) directly:

```go
var Nodes = map[int]string{
	1: "192.168.1.51:50051", // Device 1
	2: "192.168.1.52:50051", // Device 2
	3: "192.168.1.53:50051", // Device 3
	4: "192.168.1.50:50051", // Master
}
```

### Option B: The "Shell Profile" way
Add this to your `~/.zshrc` or `~/.bashrc` on all machines:
```bash
export FREEFS_NODES="1=192.168.1.51:50051,2=192.168.1.52:50051,3=192.168.1.53:50051,4=192.168.1.50:50051"
```

---

## 4. Firewall & Connectivity
For sockets to connect across machines, you MUST allow traffic on port `50051`.

- **macOS**: `System Settings -> Network -> Firewall` (Ensure it's Off or allows the `node_bin`).
- **Windows**: `Windows Defender Firewall -> Advanced Settings -> Inbound Rules` (Allow TCP port `50051`).

**Verification Test**:
From one machine, try to reach the other's socket:
```bash
telnet 192.168.1.51 50051
```
If the screen goes blank/connects, your socket programming setup is working perfectly!
