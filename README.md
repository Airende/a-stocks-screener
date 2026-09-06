# ARD选股系统

单文件 FastAPI 应用 (`app.py`) + 单页前端 (`static/index.html`)。数据来源为新浪财经公开行情接口。

功能: 多条件选股 / 均线形态筛选 / 上试盘策略三池 / 个股深度分析(买卖点·量价体系·KDJ体系·监管异动) / 组合与单股回测 / 持仓与标记管理 / 大盘快照。

## 本地运行

```bash
bash start.sh        # macOS / Linux (自动建 venv 装依赖, 默认端口 8000)
start.bat            # Windows
```

访问 http://127.0.0.1:8000

## 部署到 Render (免费)

仓库已含 `render.yaml` (Blueprint):

1. [render.com](https://render.com) 用 GitHub 登录 → **New + → Blueprint** → 选择本仓库 → Apply
2. 部署时会提示填写环境变量 `DATABASE_URL` (见下节), 可先留空, 之后在 Settings → Environment 补填并 Manual Deploy
3. 完成后得到 `https://<服务名>.onrender.com`

### 绑定自定义域名 (以 airende.xyz 为例)

1. Render 服务 → Settings → Custom Domains → 添加 `airende.xyz` 与 `www.airende.xyz`
2. 到域名注册商 DNS 控制台添加:

   | 类型 | 主机记录 | 记录值 |
   |------|---------|--------|
   | A    | `@`     | `216.24.57.1` |
   | CNAME| `www`   | `<服务名>.onrender.com` |

3. 等待 DNS 生效, Render 自动签发 HTTPS 证书

## 持久化: 持仓与标记上云 (DATABASE_URL)

Render 免费档磁盘是临时的, 重启/重新部署会丢失运行中写入的 `data/positions.json`(持仓) 与 `data/stock_marks.json`(标记)。通过 `DATABASE_URL` 环境变量启用云端存储:

- **Supabase** (免费 500MB): [supabase.com](https://supabase.com) 建项目 → Project Settings → Database → Connection string (URI, 用 Connection Pooling 的 6543 端口串) → 填入 Render 环境变量 `DATABASE_URL`
- **Neon** (免费): [neon.tech](https://neon.tech) 建项目 → 复制 connection string → 同上
- 表会自动创建 (`kv_state`), 首次启动自动把仓库内已有的本地数据迁移上云; 本地文件始终同步写一份作为备份
- **未配置 `DATABASE_URL` 时回退本地文件模式**, 行为与旧版完全一致

验证: 访问 `/api/health`, `storage` 字段为 `postgres`/`sqlite`/`file`。

```sql
-- 数据形状 (自动创建, 无需手动执行)
CREATE TABLE kv_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '');
-- key = 'marks' | 'positions', value = 整包 JSON
```

## 免费档注意事项

- 15 分钟无访问会休眠, 冷启动约 30~60 秒 (启动时会自动全市场预热扫描); 可用 cron-job.org 等每 10 分钟 ping `/api/health` 保活
- 海外托管, 国内访问无需备案但速度一般

## API 概览

`/api/screen*` 选股 · `/api/ma-screen*` 均线形态 · `/api/ssp-*` 上试盘 · `/api/stock/analyze` 个股分析 · `/api/backtest` 组合回测 · `/api/stock/backtest_single` 单股回测 · `/api/marks*` 标记 · `/api/position*` 持仓 · `/api/market-snapshot` 大盘快照 · `/api/events` SSE 状态流
