# linkwork-executor

`linkwork-executor` 是 LinkWork 任务执行器，负责从 Redis 队列消费任务、准备工作区、调用 `AgentEngine`、处理中断与归档，并在空闲超时后执行生命周期回收。

## 核心入口

- Python 包入口：`linkwork_executor.Worker`
- CLI 入口：`linkwork-executor-worker`
- 默认配置路径：`/opt/agent/config.json`

## 本地开发

### 1) 环境要求

- Python 3.11+
- Redis

### 2) 安装

```bash
cd linkwork-agent-sdk && pip install -e .
cd ../linkwork-executor && pip install -e .
```

### 3) 启动 Worker

```bash
cd linkwork-executor
WORKSTATION_ID=ws-demo \
REDIS_URL=redis://127.0.0.1:6379 \
linkwork-executor-worker --config ./config.json
```

必需环境变量：

- `WORKSTATION_ID`

常用环境变量：

- `REDIS_URL`（默认 `redis://redis:6379`）
- `IDLE_TIMEOUT`
- `TASK_RUNTIME_IDLE_TIMEOUT`
- `WORKER_DESTROY_API_BASE`
- `WORKER_DESTROY_API_PASSWORD`

## Deploy 流程

### 方案 A：随角色镜像运行（主路径）

`LinkWork/back` 构建阶段会把 `linkwork-executor` 源码复制到镜像中；运行阶段由以下脚本拉起：

- 单容器：`start-single.sh`
- 双容器（Agent + Runner）：`start-dual.sh`

脚本会完成权限初始化、`zzd` 启动、Worker 启动与优雅退出管理。

### 方案 B：独立打包发布（可选）

```bash
cd linkwork-executor
python -m build
# twine upload dist/*  # 按内部流程发布
```

## 与其他模块关系

- 依赖：`linkwork-agent-sdk`
- 上游调度：`LinkWork/back`（任务下发与状态管理）
- 数据通道：Redis 队列 + Redis Stream
