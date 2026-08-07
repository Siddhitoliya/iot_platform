# Self-Healing Medical Oxygen Pipeline

## Overview
Self-healing event pipeline for monitoring liquid medical oxygen tanks using Redis Streams with DLQ support.

## Architecture
Generator → Redis Stream → Worker → PostgreSQL
↓
DLQ Stream → Replay Service
↓
Redis Commander / Grafana

## Quick Start

```bash
# Start everything
docker-compose up -d

# Run generator
docker-compose exec generator python stream_generator.py --interval 5

# Check stats
docker-compose exec stream-worker python stream_processor.py --mode stats

# View dashboards
open http://localhost:3000  # Grafana
open http://localhost:8081  # Redis Commander
```

## Monitoring 

- Grafana: http://localhost:3000 (admin/admin)
- Redis Commander: http://localhost:8081
- Prometheus: http://localhost:9090

## Commands

# Health check
python scripts/health_check.py

# Load test
python scripts/load_test.py --clients 20 --messages 50

# Monitor streams
python scripts/monitor_streams.py

## License
MIT