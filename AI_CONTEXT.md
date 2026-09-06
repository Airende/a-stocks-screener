# AI_CONTEXT.md — AI 助手交接文档

> 本文档由 AI 助手于 2026-09-06 整理，供后续 AI（或人类）快速了解项目现状、
> 本会话完成的全部工作、踩过的坑以及待办事项。**接手前请完整阅读。**

## 一、项目概述

**ARD选股系统**（原名"A股智能选股系统"，2026-09-06 更名）—— A股个人选股/复盘工具。

- **技术栈**: Python FastAPI 单文件后端(`app.py` ≈7100行) + 单页原生JS前端(`static/index.html` ≈4700行)
- **数据源**: 新浪财经(主) → 腾讯(备) → 东方财富(第三), 三级容灾自动切换
- **启动**: `bash start.sh`(自动建venv) 或 `.venv/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8000`
- **依赖**: requirements.txt = fastapi / uvicorn / requests / pypinyin / numpy / psycopg2-binary

### 文件地图
| 文件 | 内容 |
|------|------|
| `app.py` | 全部后端: 数据层→指标→选股/均线/上试盘三套筛选→分析引擎→回测×2→标记/持仓→SSE |
| `static/index.html` | 单页前端(内联JS≈2500行), tab: 均线形态/选股条件/个股分析/标记管理/大盘云图/历史信号 |
| `data/*.json` | 标记+持仓(本地镜像, 云端为权威源, 见§四) |
| `cache/` | K线按日缓存/klines/<YYYYMMDD>/, 自动清理保留5天; signal_history.json 信号归档 |
| `deploy/server-setup.sh` | 云服务器一键部署(Ubuntu+Caddy自动HTTPS+systemd) |
| `.env` | DATABASE_URL(**gitignore, 不入库**), 见§四 |

### 核心机制速查
- **选股条件**: COND_DEFS(gA趋势/gB启动/gC买点/gD风控排除/gE波动ATR%), 单一数据源,
  GATE_CONDS={d3..d6}为硬剔除门; 默认勾选集=COND_DEFAULT(全部-e1-e3, 即ATR组默认只勾e2)
- **标记体系**: `" `* `!` 三级 + removed忽略态(名称后绿色×, 点击弹标记菜单)
- **持仓**: 买卖操作流水+撤销缓冲(_UNDO_BUFFER 50条), 重算函数 _recompute_position
- **SSE**: GET /api/events 每2秒推送 screen/ma/ssp 三模块状态, 前端EventSource+轮询兜底
- **容灾**: 熔断器 _SOURCE_BREAKER(全市场级失败立即熔断600s, 单只K线60s内≥5次才熔断),
  **绝不能在熔断打开期间再续期熔断**(曾致全站瘫痪, 见§五)

## 二、基础设施状态(2026-09-06)

| 项 | 状态 |
|----|------|
| Supabase | ✅ 项目 `oxsqqngljiuvrfmbclio`(新加坡, 免费), **Session pooler** 连接串(端口5432, IPv4兼容) |
| 本机(用户Mac) | ✅ `.env` 已配置, storage=postgres, 数据已合并同步(标记36条/持仓3只) |
| 服务器 | ❌ **未定**: 阿里云38元/年秒杀(10点/15点,新用户,限量,首次尝试失败) / 99计划99元/年 / RackNerd美国$35.99/年 / 腾讯云香港~¥35/月。用户嫌¥56/月贵 |
| 域名 airende.xyz | 已购, DNS未指向(等服务器)。内地服务器需ICP备案(1-2周), 香港免备案 |
| Render | ❌ 已放弃: 新账号强制绑卡验证身份, 用户不愿 |
| GitHub SSH | ✅ 新部署密钥 `~/.ssh/a_stocks_deploy_key`(已加入GitHub账号), `~/.ssh/config` 已配置指向它。**旧 id_ed25519 已被用户从GitHub移除**(账号安全加固时) |

⚠️ Supabase 连接串含密码, 在 `.env` 和本会话记录中出现过——用户已被建议改密码。

## 三、本会话完成的全部工作(22个提交, 均已推送)

### Bug修复(共9个, 详见§五避坑)
- `dc3885f` SSP定时线程 `_ssp_log` 未定义致崩溃 + `updated`赋值引用错误作用域变量
- `2cd840a` requirements补numpy(缺失致指数快照/上试盘静默失败) + 持仓/回测两模块
  `_is_limit_up`重名遮蔽致单股回测500 → 改名`_spot_is_limit_up`
- `f5f1695` 前端openStock多余`}`致整个内联script解析失败(页面卡"加载中")
- `b3d52ac` 熔断自续期死循环(熔断期失败又续期熔断,主源恢复也不可用)
- `34889f3` 悬停预览maCalc减法漏`.close`(减K线对象→NaN,均线只剩孤立点)
- `0610353` /api/kline名称读取: universe文件是list不能用只认dict的读取器(两次踩同类坑)

### 新功能
- **云端存储**(f7b7610): kv_state表抽象, DATABASE_URL支持postgres/sqlite/未配置(文件模式),
  云端优先+本地镜像双写+自动迁移上云; **多机同步**(d57a9c4): .env自动加载 +
  _merge_newer逐记录updated_at新者胜合并(两台电脑互不覆盖)
- **每日信号归档+历史信号复盘页**(同批): screen/ma/ssp按日归档, /api/history接口, 前端新tab
- **行情源容灾**(同批): 新浪→腾讯→东财三级链, K线量手→股×100/不复权对齐新浪,
  spot备源用universe_latest.json股票池
- **磁盘缓存自动清理**(同批): 启动+每日9点, 保留5天
- **ATR选股条件**(3ba3a7e等): 新增gE组(e1<4%趋势/e2 3~8%波段默认/e3收缩蓄势)满足其一;
  d8排除>8%曾加后按用户要求**彻底删除**; 剔除门d7(科创板)也**彻底删除**——
  688与ATR>8%的票现在正常参筛(c4e93ba)
- **历史时点复盘**(70e0eae/3a63725): /api/stock/analyze?date=, K线截断到指定日,
  全指标按时点重算; 前端日期选择器+基准日徽章+回到今天按钮
- **悬停K线预览**(9d55fbe→a01acb1→ce10288→34889f3): 悬停代码350ms弹出三面板
  (K线+MA5/10/20 / 成交量+VR / KDJ), 全面板十字光标+自定义悬浮, 延迟隐藏300→200→100ms(用户迭代)
- **强制刷新**(af4141c): 刷新K线数据按钮清磁盘+内存全部缓存
- **收盘后缓存策略**(4a4c678): 先验证今日bar再落盘, 源滞后用历史数据不锁死缓存
- **UI 11项**(eb68419): 进度条/分页/信号徽章/窄屏适配/状态条/SSE/十字光标等
- **弱市确认池**(df6dcea): 沪深300破MA20仅红横幅警示, 确认池保留不剔除(用户明确要求, 曾两次反转)
- **其他样式**: 看跌tab绿色(10a9c0e)/吸顶头/背景统一/忽略×绿色/产品更名(4ea418f)/股票名粗体
- **选股结果表**(00fd1cc): 删预估5/10/20日列, 加ATR%列

## 四、数据与同步语义(重要)

- **云端为权威源**(配置DATABASE_URL时): 启动时云端与本地文件逐记录按updated_at
  新者合并→合并结果回推云端+写本地镜像。**两台电脑分别改动互不覆盖**。
- `.env`(gitignore)内容 = `DATABASE_URL=postgresql://postgres.oxsqqngljiuvrfmbclio:Stk-Airende2026%21Pg@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres`
- **新电脑接入**: git pull → 项目根创建上述.env → 启动 → 日志见"云端存储已启用: postgres"即成功
- 未配DATABASE_URL = 文件模式, data/*.json仅本地(git里提交的是旧快照, 勿当作最新)

## 五、避坑清单(本会话真实踩过, 勿重犯)

1. **容灾/降级路径的依赖链必须完整测试**: 一天连出4个bug全是降级路径的
   (熔断阈值敏感/股票池list当dict读×2次/熔断自续期)——修"主路径"时降级分支更容易藏错
2. **同类类型坑**: `_kv_read_local_file`只认dict; universe/signal文件是list——
   项目里有多种文件形状, 读文件前先确认形状
3. **前端验证不要用颜色过滤path**(KDJ的D/J与MA5/MA10同色会误判)——直接解剖目标svg的path
4. **内联JS单文件**: 任何改动静后必跑 `node --check` 提取法(见§七); 曾因一个多余`}`全站瘫痪
5. **同名函数遮蔽**: app.py单文件7000行, 新增函数前先grep全名(_is_limit_up前车之鉴)
6. **`xxx in dir()`判断作用域恒为False**(SSP的updated bug); 嵌套函数局部变量别在外层引用
7. **服务器本地时区≠北京时间**(部署海外后date.today()会错天), 统一用bj_now()
8. **sqlite3.Cursor不支持with上下文**(psycopg2支持)——跨两种DB的代码用裸cursor
9. **React/自绘组件的disabled按钮**: JS强点无效属正常, 先查是否资格/时间窗问题

## 六、代码/提交约定

- 注释带日期 `20260906 用户改/修复: ...`; 被废弃逻辑保留为注释不删除
- 提交: `feat:/fix:/style:/chore:` 前缀+中文详述; data/*.json 运行数据随仓库提交
- 用户偏好: 直接给选项让他选(AskUserQuestion); 支付/实名/验证码等敏感操作**必须用户自己做**;
  改动要实测验证后再交付; 用户会快速连续提小需求(如延迟参数迭代), 保持最小改动

## 七、给下一个AI的操作提示

```bash
# 语法检查(改完必做)
python3 -m py_compile app.py
python3 -c "import re;open('/tmp/s.js','w').write(re.findall(r'<script[^>]*>(.*?)</script>', open('static/index.html',encoding='utf-8').read(), re.S)[0])"
node --check /tmp/s.js
# 启动(本机)
.venv/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8000   # 已有.env, 自动postgres模式
# 冒烟
curl http://127.0.0.1:8000/api/health   # storage字段=postgres|sqlite|file
curl http://127.0.0.1:8000/api/screen   # 首次约1-2分钟
```

## 八、待办

- [ ] 服务器购买(方案未定, 见§二) → 跑 deploy/server-setup.sh → DNS指向 airende.xyz
- [ ] 若买内地服务器: 需ICP备案(阿里云要求包年包月≥3个月)
- [ ] 用户GitHub/数据库密码已建议轮换(本会话中明文出现过); 建议开2FA
- [ ] Render方案已弃用(强制绑卡); 其提交的render.yaml仍保留
