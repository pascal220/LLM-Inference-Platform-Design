# LLM Inference Platform — Prototype

A prototype implementation of a scalable LLM inference platform supporting
multi-tenancy, priority queuing, token streaming via SSE, and per-tenant
rate limiting.

## Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd llm-inference-platform

# 2. Copy environment file
cp .env.example .env

# 3. Start all services
docker-compose up --build

# 4. Seed tenant data
curl -X POST http://localhost:8003/tenants \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "acme",
    "api_key": "key-acme-premium",
    "tier": "premium",
    "rate_limit_rps": 100,
    "system_prompt": "You are a helpful assistant for AcmeCorp."
  }'

curl -X POST http://localhost:8003/tenants \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "beta-corp",
    "api_key": "key-beta-standard",
    "tier": "standard",
    "rate_limit_rps": 10,
    "system_prompt": ""
  }'

# 5. Send a test request (streaming)
curl -N http://localhost:8000/v1/chat \
  -H "Authorization: Bearer key-acme-premium" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{"role": "user", "content": "Hello, how are you?"}],
    "max_tokens": 100
  }'
```

## Services

| Service           | Port | Description                        |
|-------------------|------|------------------------------------|
| API Gateway       | 8000 | Auth, rate limiting, SSE proxy     |
| Inference Gateway | 8001 | Queue routing, SSE stream manager  |
| Worker            | 8002 | Token generation (mock or vLLM)    |
| Control Plane     | 8003 | Tenant config, health monitoring   |
| Prometheus        | 9090 | Metrics scraping                   |
| Grafana           | 3000 | Metrics dashboard (admin/admin)    |

## Running Tests

```bash
pip install pytest pytest-asyncio httpx
pytest tests/ -v
```

## Running Load Tests

```bash
pip install locust
locust -f tests/load_test.py --host=http://localhost:8000
```

## Switching to Real vLLM Inference

1. Set `USE_MOCK=false` in `.env`
2. Set `MODEL_PATH=TinyLlama/TinyLlama-1.1B-Chat-v1.0`
3. Uncomment the GPU section in `docker-compose.yml`
4. Rebuild: `docker-compose up --build worker`