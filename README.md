# linkwork-executor

English | [中文](./README_zh-CN.md)

`linkwork-executor` is the LinkWork task worker. It consumes tasks from Redis queues, prepares workspace context, invokes `AgentEngine`, handles interrupts/archive, and performs lifecycle scale-down on idle timeout.

## Entry Points

- Python package entry: `linkwork_executor.Worker`
- CLI entry: `linkwork-executor-worker`
- Default config path: `/opt/agent/config.json`

## Local Development

### 1) Requirements

- Python 3.11+
- Redis

### 2) Install

```bash
cd linkwork-agent-sdk && pip install -e .
cd ../linkwork-executor && pip install -e .
```

### 3) Start worker

```bash
cd linkwork-executor
WORKSTATION_ID=ws-demo \
REDIS_URL=redis://127.0.0.1:6379 \
linkwork-executor-worker --config ./config.json
```

Required env var:

- `WORKSTATION_ID`

Common env vars:

- `REDIS_URL` (default: `redis://redis:6379`)
- `IDLE_TIMEOUT`
- `TASK_RUNTIME_IDLE_TIMEOUT`
- `WORKER_DESTROY_API_BASE`
- `WORKER_DESTROY_API_PASSWORD`

## Deploy Flow

### Option A: Run inside role image (primary path)

In `LinkWork/back`, build flow copies `linkwork-executor` source into role images. Runtime is started through:

- `start-single.sh` (single-container mode)
- `start-dual.sh` (agent + runner mode)

These scripts manage permission setup, `zzd` startup, worker startup, and graceful shutdown.

### Option B: Publish as standalone package (optional)

```bash
cd linkwork-executor
python -m build
# twine upload dist/*   # use your internal release process
```

## Related Components

- Depends on `linkwork-agent-sdk`
- Upstream scheduler: `LinkWork/back`
- Data channel: Redis queue + Redis stream
