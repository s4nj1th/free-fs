# Running FREE-FS across Multiple Devices

To run the FREE-FS demo on 4 different devices (e.g. 4 laptops on the same Wi-Fi network), you need to update the node IP configurations dynamically.

I've updated the codebase so you can pass the IP addresses of all your devices using the `FREEFS_NODES` environment variable.

### Prerequisites
- 4 Devices connected to the same network.
- Go installed on each device.
- Find out the IP address of each device on the local network (e.g. using `ifconfig` or `ipconfig`). Let's assume:
  - Device 1 (Node 1): `192.168.1.101`
  - Device 2 (Node 2): `192.168.1.102`
  - Device 3 (Node 3): `192.168.1.103`
  - Device 4 (Master): `192.168.1.104`

### Step 1: Build the node binary
On all devices, build the node executable:
```bash
go build -o node_bin cmd/node/main.go
```

### Step 2: Define the Network Variable
For all devices, you'll need the exact same connection string that maps the `NODE_ID` to their respective IP addresses on port `50051`.

```bash
export FREEFS_NODES="1=192.168.1.101:50051,2=192.168.1.102:50051,3=192.168.1.103:50051,4=192.168.1.104:50051"
```

### Step 3: Run the Nodes
Start the servers simultaneously on their respective computers, providing their specific Node ID.

**On Device 1:**
```bash
NODE_ID=1 ./node_bin
```

**On Device 2:**
```bash
NODE_ID=2 ./node_bin
```

**On Device 3:**
```bash
NODE_ID=3 ./node_bin
```

**On Device 4 (The Initial Master):**
```bash
NODE_ID=4 ./node_bin
```

### Step 4: Run the Client from any device
You can create a client and interact with the distributed cluster. Let's build the client on one of the devices (e.g., Device 1):

```bash
go build -o client_bin cmd/client/main.go
```

Then create a test file locally:
```bash
echo "Hello from physical devices!" > /tmp/hello.txt
```

Put the file into your distributed FREE-FS array:
```bash
# Don't forget to pass in the FREEFS_NODES env var so the client knows how to contact the Master!
./client_bin put /tmp/hello.txt /remote_hello.txt
```

Verify it's there:
```bash
./client_bin ls
./client_bin stat /remote_hello.txt
```

Get the file back:
```bash
./client_bin get /remote_hello.txt /tmp/downloaded_hello.txt
cat /tmp/downloaded_hello.txt
```

### Demonstrating Failure Recovery

Try killing the node running `NODE_ID=4` (The Master). 
Watch the terminal output on the remaining 3 devices. They will detect the heartbeat failure after 10 seconds, run an election, and the node with the next highest ID (Node 3) will announce itself as the new Master!
