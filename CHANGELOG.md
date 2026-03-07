# Changelog

All notable changes to FREE-FS are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [1.0.0] — Initial Release

### Added

- Master server with full GFS metadata management (file→chunk mappings, namespace, persistence)
- ChunkServer with gRPC read/write/replicate/delete
- 3× replication factor with automatic re-replication on server failure
- Heartbeat monitoring (10s interval, 30s timeout)
- Hierarchical namespace with directory support (`mkdir`, `ls`, `mv`)
- Master state persistence to JSON with atomic writes
- Python CLI with animated progress bars, spinners, and live cluster dashboard
- Interactive shell mode (`free-fs shell`)
- Full cluster demo mode (`free-fs demo`)
- Docker Compose cluster: 1 master + 3 chunk servers
- Multi-machine deployment support via docker-compose.multi.yml
- GitHub Actions CI: lint, build (5 platforms), docker build, integration test
- GitHub Actions Release: cross-platform binaries + GHCR Docker images
- MIT License
