# CapitalPulse

行业板块实时资金流看板。项目采集行业板块资金流数据，通过实时曲线展示行业主力资金强弱，并提供主力、超大单、大单、中单和小单的行业细分资金流向。

## 功能特性

- 申万二级行业 Top 30 实时资金流监控
- 主力资金累计曲线总览
- 最近 30 个交易日的主力资金净流入柱状图，Top 30 行业按每页 6 个分页展示
- 主力、超大单、大单、中单、小单五类资金曲线
- 行业细分曲线分页展示，每页 6 个行业
- WebSocket 实时更新、端点提示与更新闪烁
- 盘中历史分钟数据自动补全
- SQLite 本地持久化，默认保留最近 30 天数据
- 支持浅色和深色系统主题
- 响应式布局，兼容桌面端和移动端

## 技术栈

### 前端

- Next.js 16
- React 19
- TypeScript
- Tailwind CSS 4
- Apache ECharts 5
- Lucide React

### 后端

- Python 3.11+
- FastAPI
- Uvicorn
- HTTPX
- SQLite

## 项目结构

```text
CapitalPulse/
├── backend/
│   ├── routers/                    # REST API 路由
│   ├── services/                   # 行情采集、历史回填与持久化
│   ├── tests/                      # 后端单元测试
│   ├── utils/                      # 行业筛选等工具
│   ├── config.py                   # 上游接口配置
│   ├── main.py                     # FastAPI 入口
│   └── requirements.txt
├── frontend/
│   ├── public/                     # Logo 等静态资源
│   └── src/app/                    # Next.js 页面与样式
├── .gitignore
├── bun.lock                        # Bun 依赖锁定文件
├── LICENSE
└── package.json
```

运行时数据库默认保存在 `backend/data/sector_flow_realtime.sqlite3`。该目录已被 Git 忽略，不会随源代码提交。

## 环境要求

- Anaconda 或 Miniconda
- Bun 1.2+
- Node.js 20+
- Python 3.11+

## 使用 Anaconda 安装

打开 Anaconda Prompt 或已经初始化 Conda 的终端，进入项目根目录并创建独立环境：

```powershell
conda create -n capitalpulse python=3.11 -y
conda activate capitalpulse
```

安装后端 Python 依赖：

```powershell
python -m pip install --upgrade pip
python -m pip install -r backend\requirements.txt
```

安装前端依赖：

```powershell
bun install
```

确认环境安装成功：

```powershell
conda info --envs
python --version
node --version
bun --version
```

以后重新打开终端时，只需进入项目根目录并执行 `conda activate capitalpulse`，不需要重复安装依赖。

## 配置

项目内提供了以下示例配置：

- `backend/.env.example`
- `frontend/.env.example`

后端配置项：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SECTOR_FLOW_ENABLED` | `true` | 是否启用行业资金采集 |
| `SECTOR_FLOW_POLL_SECONDS` | `3` | 交易时段轮询间隔，单位为秒 |
| `SECTOR_FLOW_DB_PATH` | `backend/data/sector_flow_realtime.sqlite3` | SQLite 数据库路径 |
| `SECTOR_FLOW_RETENTION_DAYS` | `30` | 数据保留天数 |

前端配置项：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `VANE_API_URL` | `http://localhost:8000` | Next.js 服务端代理的后端地址 |
| `NEXT_PUBLIC_SECTOR_FLOW_WS_URL` | 自动根据浏览器地址生成 | 可选的 WebSocket 公网地址 |

本地开发使用默认值即可启动。部署时请通过运行环境注入后端变量，并将前端变量放入 `frontend/.env.local` 或部署平台的环境变量配置中。不要提交包含真实凭据的 `.env` 文件。

## 本地开发启动

分别打开两个 Anaconda Prompt 或 Conda 终端，并进入项目根目录。

终端一启动后端：

```powershell
conda activate capitalpulse
bun run dev:api
```

终端二启动前端：

```powershell
conda activate capitalpulse
bun run dev:web
```

启动后访问：

- 前端：<http://localhost:3000>
- 后端 API：<http://localhost:8000>
- API 文档：<http://localhost:8000/docs>

## 使用 Anaconda 生产部署

生产环境同样使用 `capitalpulse` Conda 环境。首先安装依赖并构建前端：

```powershell
conda activate capitalpulse
python -m pip install -r backend\requirements.txt
bun install --frozen-lockfile
bun run build
```

构建完成后，分别启动后端和前端服务。

终端一启动 FastAPI：

```powershell
conda activate capitalpulse
python -m uvicorn main:app --app-dir .\backend --host 0.0.0.0 --port 8000
```

终端二启动 Next.js：

```powershell
conda activate capitalpulse
$env:VANE_API_URL="http://127.0.0.1:8000"
bun run start
```

服务器需要放行或反向代理以下端口：

- `3000`：前端页面
- `8000`：FastAPI、REST API 和 WebSocket

如果通过域名和 HTTPS 部署，请在 `frontend/.env.local` 中设置公网 WebSocket 地址，然后重新构建前端：

```dotenv
NEXT_PUBLIC_SECTOR_FLOW_WS_URL=wss://example.com/ws/sector-flow
```

建议使用 Nginx、Caddy 或其他反向代理统一暴露 HTTPS，并确保 `/ws/sector-flow` 支持 WebSocket 升级。SQLite 数据库默认位于 `backend/data/sector_flow_realtime.sqlite3`，部署时应为该目录配置持久化存储和写权限。

## API

| 方法 | 地址 | 说明 |
| --- | --- | --- |
| `GET` | `/api/health` | 后端健康检查 |
| `GET` | `/api/sector-flow/status` | 采集器和市场状态 |
| `GET` | `/api/sector-flow/history?top=30` | 行业主力资金历史曲线 |
| `GET` | `/api/sector-flow/detail-history?page=1` | 每页 6 个行业的五类资金历史曲线 |
| `GET` | `/api/sector-flow/daily-history?top=30&days=30&page=1` | 每页 6 个行业最近 30 个交易日的五类资金日频曲线 |
| `WS` | `/ws/sector-flow` | 行业资金实时推送 |

历史接口支持可选的 `trade_date=YYYY-MM-DD` 参数。数据日期和交易时段按中国标准时间处理。

## 数据与存储

- 每个交易日选取申万二级行业中市值排名前 30 的行业。
- 交易时段默认每 3 秒采集一次累计资金快照。
- 实时快照与分钟回填数据统一保存在 SQLite 中。
- 30 日视图使用上游日频资金数据并落盘到 SQLite；缓存缺失时补拉，交易日收盘后由后台自动更新当天数据。
- 日频缓存按行业保留最新 30 个交易日，已不在保留期行业名单中的缓存会在启动时自动清理。
- 相同时间、相同行业的数据采用唯一键去重。
- 旧数据根据 `SECTOR_FLOW_RETENTION_DAYS` 自动清理。
- 数据库、运行日志、生成图表和视频均不会提交到 Git。

## 测试与构建

运行后端测试：

```bash
bun run test:api
```

检查前端代码：

```bash
bun run lint
```

构建前端生产版本：

```bash
bun run build
```

## 数据来源与风险提示

本项目基于公开行情接口整理行业资金流数据，与行情数据提供方不存在隶属、授权或官方合作关系。上游接口可能随时调整、延迟或停止服务，使用者应自行遵守数据提供方的服务条款和请求频率限制。

本项目仅用于技术研究和数据展示，不保证数据的准确性、完整性与实时性，不构成任何投资建议。因使用本项目产生的交易、投资或其他损失，由使用者自行承担。

## 参与贡献

欢迎通过 Issue 报告问题或提出建议。提交 Pull Request 前，请确保：

1. 不包含数据库、环境变量、密钥或其他敏感数据。
2. 后端测试全部通过。
3. 前端 lint 和生产构建通过。
4. 新增功能附带必要的测试或验证说明。

## License

本项目采用 [MIT License](LICENSE) 开源。
