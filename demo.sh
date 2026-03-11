#!/bin/bash
set -e

echo "Building and starting FREE-FS cluster..."
docker compose build
docker compose up -d

echo "Waiting for cluster to initialize..."
sleep 5

echo "Creating a test file of 2.5MB to split into 3 chunks..."
docker exec free-fs-client-1 sh -c "head -c 2621440 /dev/urandom > /tmp/testfile.bin"

echo "Putting file into FREE-FS..."
docker exec free-fs-client-1 /client put /tmp/testfile.bin /remote_test.bin

echo "Listing files..."
docker exec free-fs-client-1 /client ls

echo "Getting file stat..."
docker exec free-fs-client-1 /client stat /remote_test.bin

echo "Reading file back..."
docker exec free-fs-client-1 /client get /remote_test.bin /tmp/downloaded.bin

echo "Validating downloaded file size..."
docker exec free-fs-client-1 ls -lh /tmp/downloaded.bin
docker exec free-fs-client-1 ls -lh /tmp/testfile.bin

echo "Killing Master (Node 4) to trigger Bully Election..."
docker compose stop master
echo "Waiting 12 seconds for heartbeat timeout and election..."
sleep 12

echo "Listing files again with new Master (will be empty per requirement of no persistence)..."
docker exec free-fs-client-1 /client ls

echo "Killing chunkserver-1 (Node 1) to trigger heartbeat failure detection..."
docker compose stop chunkserver-1
echo "Waiting 12 seconds for new Master to mark it DEAD..."
sleep 12

echo "Check docker compose logs to see the new Master detecting chunkserver failure:"
docker compose logs --tail=20 chunkserver-3

echo "Demo complete!"
