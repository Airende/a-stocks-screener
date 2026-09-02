# -*- coding: utf-8 -*-
"""
A股选股系统 - 后端
数据来源: 新浪财经 (经沙箱代理可达; 东方财富在该代理下不可用)
提供:
  - 全市场实时行情快照 (流通市值/成交额/今日涨跌)
  - 个股日K线 (含当日, 用于均线/KDJ/形态计算)
  - 申万一级行业 + 概念板块 映射
  - 选股条件筛选 (量价/均线趋势/KDJ/形态/热度龙头)
  - 近5/10/20日涨幅预估 (技术信号推算, 仅供参考)
"""
from __future__ import annotations

import json
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any

# 北京时间 (UTC+8)
_BJ_TZ = timezone(timedelta(hours=8))


def bj_now(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """返回北京时间格式化字符串"""
    return datetime.now(_BJ_TZ).strftime(fmt)

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# ============================================================
# 配置
# ============================================================
SINA_HQ = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
SINA_NODES = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodes"
SINA_KLINE = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
SINA_NODE_COUNT = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount"

REFERER = "https://finance.sina.com.cn"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

HEADERS = {"Referer": REFERER, "User-Agent": UA}

# 线程池: 用于并发拉取行情/K线
POOL = ThreadPoolExecutor(max_workers=40)

# 缓存
_CACHE = {}
_CACHE_LOCK = threading.Lock()
# 行业/概念映射缓存 (构建一次, 当日内复用)
_BOARD_MAP_TTL = 6 * 3600  # 6 小时
_board_cache = {"built_at": 0.0, "industry": {}, "concept": {}}

# ============================================================
# 本地文件缓存 (收盘后行情/K线持久化, 减少重复拉取)
# ============================================================
import os

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")


def _bj_date_str() -> str:
    """返回北京时间日期字符串 YYYYMMDD"""
    return datetime.now(_BJ_TZ).strftime("%Y%m%d")


def _is_after_close() -> bool:
    """判断当前北京时间是否收盘后 (>=15:00)"""
    now_bj = datetime.now(_BJ_TZ)
    # 周末不算交易日, 收盘判断只针对工作日
    return now_bj.weekday() < 5 and now_bj.hour >= 15


def _latest_trade_date_str() -> str:
    """返回最近交易日日期字符串 (跳过周末)"""
    now_bj = datetime.now(_BJ_TZ)
    # 如果是工作日且 >=15:00, 返回今天; 否则回退到最近的工作日
    while now_bj.weekday() >= 5:  # 周六周日回退
        now_bj -= timedelta(days=1)
    if now_bj.weekday() < 5 and datetime.now(_BJ_TZ).hour < 15 and now_bj.date() == datetime.now(_BJ_TZ).date():
        # 盘前, 用昨天(回退到最近工作日)
        now_bj -= timedelta(days=1)
        while now_bj.weekday() >= 5:
            now_bj -= timedelta(days=1)
    return now_bj.strftime("%Y%m%d")


def _spot_cache_path(date_str: str) -> str:
    return os.path.join(CACHE_DIR, f"spot_{date_str}.json")


def _kline_cache_dir(date_str: str) -> str:
    return os.path.join(CACHE_DIR, "klines", date_str)


def _kline_cache_path(date_str: str, symbol: str) -> str:
    return os.path.join(_kline_cache_dir(date_str), f"{symbol}.json")


def _load_spot_cache(date_str: str) -> list[dict] | None:
    path = _spot_cache_path(date_str)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
    return None


def _save_spot_cache(date_str: str, data: list[dict]) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _spot_cache_path(date_str)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except OSError:
        pass


def _load_kline_cache(date_str: str, symbol: str) -> list[dict] | None:
    path = _kline_cache_path(date_str, symbol)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
    return None


def _save_kline_cache(date_str: str, symbol: str, data: list[dict]) -> None:
    d = _kline_cache_dir(date_str)
    os.makedirs(d, exist_ok=True)
    path = _kline_cache_path(date_str, symbol)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except OSError:
        pass


def _cache_date_for_fetch() -> str:
    """返回数据缓存使用的日期: 收盘后用今天, 盘前/盘中用最近交易日"""
    now_bj = datetime.now(_BJ_TZ)
    # 盘前(<15:00)或周末: 回退到最近的工作日
    if now_bj.hour < 15:
        now_bj -= timedelta(days=1)
    while now_bj.weekday() >= 5:  # 周六周日继续回退
        now_bj -= timedelta(days=1)
    return now_bj.strftime("%Y%m%d")


def _should_use_spot_cache() -> bool:
    """收盘后使用缓存; 盘中实时拉取"""
    return _is_after_close()


def _get(url: str, params: dict | None = None, timeout: int = 15) -> Any:
    """带重试的 GET 请求, 返回 JSON 或原始文本"""
    last = None
    for _ in range(3):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and r.text.strip():
                try:
                    return json.loads(r.text)
                except json.JSONDecodeError:
                    return r.text
            last = f"{r.status_code}:{r.text[:80]}"
        except Exception as e:  # noqa: BLE001
            last = str(e)
        time.sleep(0.5)
    raise RuntimeError(f"GET {url} failed: {last}")


# ============================================================
# 数据层: 全市场实时快照
# ============================================================
def fetch_spot_all() -> list[dict]:
    """拉取沪深A股 + 北交所 全部实时快照, 返回扁平化字典列表。
    收盘后优先使用本地缓存; 盘中实时拉取。"""
    # 收盘后: 优先用本地缓存
    if _should_use_spot_cache():
        cache_date = _cache_date_for_fetch()
        cached = _load_spot_cache(cache_date)
        if cached is not None:
            return cached
    # 先取总数
    total = _get(SINA_NODE_COUNT, {"node": "hs_a"})
    if not isinstance(total, (int, str)) or int(total) <= 0:
        total = 5500
    total = int(total)
    page_size = 100
    pages = (total + page_size - 1) // page_size

    def fetch_page(page: int) -> list[dict]:
        data = _get(SINA_HQ, {"page": page, "num": page_size, "node": "hs_a"})
        return data if isinstance(data, list) else []

    rows: list[dict] = []
    futs = [POOL.submit(fetch_page, p) for p in range(1, pages + 1)]
    for f in as_completed(futs):
        try:
            rows.extend(f.result())
        except Exception:  # noqa: BLE001
            continue
    # 收盘后保存缓存
    if _should_use_spot_cache() and rows:
        _save_spot_cache(_cache_date_for_fetch(), rows)
    return rows


# ============================================================
# 数据层: 个股日K线 (含当日)
# ============================================================
def fetch_kline(symbol: str, datalen: int = 40) -> list[dict]:
    """返回 [{day,open,high,low,close,volume}, ...] 含当日。
    优先使用本地缓存(按交易日); 无缓存则从服务拉取并保存。"""
    cache_date = _cache_date_for_fetch()
    # 检查本地缓存
    cached = _load_kline_cache(cache_date, symbol)
    if cached is not None and len(cached) >= datalen:
        return cached
    data = _get(SINA_KLINE, {"symbol": symbol, "scale": 240, "ma": "no", "datalen": datalen})
    if not isinstance(data, list):
        return []
    out = []
    for d in data:
        try:
            out.append({
                "day": d.get("day"),
                "open": float(d["open"]),
                "high": float(d["high"]),
                "low": float(d["low"]),
                "close": float(d["close"]),
                "volume": float(d["volume"]),
            })
        except (KeyError, ValueError, TypeError):
            continue
    # 保存缓存
    if out:
        _save_kline_cache(cache_date, symbol, out)
    return out


# ============================================================
# 数据层: 行业 + 概念板块映射
# ============================================================
def _build_board_maps():
    """构建 code -> 申万一级行业, code -> [概念板块...] 映射 (缓存)"""
    now = time.time()
    if now - _board_cache["built_at"] < _BOARD_MAP_TTL and _board_cache["industry"]:
        return
    tree = _get(SINA_NODES)
    # tree = ["行情中心", [ ["A股", [ [catname, [[item,'',code],...]], ... ]], ... ]]
    try:
        a_group = tree[1][0][1]  # "A股" 的子分类列表
    except (IndexError, TypeError):
        return
    # 找到 "申万一级" 和 "概念板块" 两个子分类
    sw_items, concept_items = [], []
    for cat in a_group:
        if not (isinstance(cat, list) and len(cat) >= 2):
            continue
        catname, children = cat[0], cat[1]
        items = [c for c in children if isinstance(c, list) and len(c) >= 3]
        if catname == "申万一级":
            sw_items = items
        elif catname == "概念板块":
            concept_items = items

    def fetch_node_members(node_code: str) -> list[str]:
        codes = []
        page = 1
        while True:
            data = _get(SINA_HQ, {"page": page, "num": 100, "node": node_code})
            if not isinstance(data, list) or not data:
                break
            for row in data:
                code = row.get("code") or (row.get("symbol") or "")[-6:]
                if code:
                    codes.append(code)
            if len(data) < 100:
                break
            page += 1
        return codes

    industry_map: dict[str, str] = {}
    concept_map: dict[str, list[str]] = {}

    # 并发拉取各节点成员 (行业 + 概念), 用锁合并结果
    lock = threading.Lock()
    all_items = [(name, code, False) for name, _, code in sw_items] + \
               [(name, code, True) for name, _, code in concept_items]

    def build_one(name, node_code, is_concept):
        try:
            members = fetch_node_members(node_code)
        except Exception:  # noqa: BLE001
            return
        with lock:
            if is_concept:
                for c in members:
                    concept_map.setdefault(c, [])
                    if name not in concept_map[c]:
                        concept_map[c].append(name)
            else:
                for c in members:
                    industry_map[c] = name

    futs = [POOL.submit(build_one, n, c, ic) for (n, c, ic) in all_items]
    for f in as_completed(futs):
        f.result()
    # 概念: 每股保留最多3个
    for c, cs in concept_map.items():
        concept_map[c] = cs[:3]

    _board_cache["industry"] = industry_map
    _board_cache["concept"] = concept_map
    _board_cache["built_at"] = now


def get_industry(code: str) -> str:
    return _board_cache["industry"].get(code, "—")


def get_concepts(code: str) -> str:
    cs = _board_cache["concept"].get(code, [])
    return "、".join(cs[:3]) if cs else "—"


# ============================================================
# 技术指标
# ============================================================
def sma(values: list[float], n: int) -> list[float]:
    out = [float("nan")] * len(values)
    if len(values) < n:
        return out
    acc = sum(values[:n])
    out[n - 1] = acc / n
    for i in range(n, len(values)):
        acc += values[i] - values[i - n]
        out[i] = acc / n
    return out


def calc_kdj(highs: list[float], lows: list[float], closes: list[float],
            n: int = 9, m1: int = 3, m2: int = 3) -> tuple[list[float], list[float], list[float]]:
    """标准 KDJ(9,3,3). 返回 (K, D, J) 序列, 与输入等长, 前部不足处为 nan"""
    k = [float("nan")] * len(closes)
    d = [float("nan")] * len(closes)
    j = [float("nan")] * len(closes)
    prev_k = 50.0
    prev_d = 50.0
    for i in range(len(closes)):
        if i < n - 1:
            continue
        hh = max(highs[i - n + 1:i + 1])
        ll = min(lows[i - n + 1:i + 1])
        rsv = 50.0 if hh == ll else (closes[i] - ll) / (hh - ll) * 100.0
        cur_k = (prev_k * (m1 - 1) + rsv) / m1
        cur_d = (prev_d * (m2 - 1) + cur_k) / m2
        cur_j = 3 * cur_k - 2 * cur_d
        k[i], d[i], j[i] = cur_k, cur_d, cur_j
        prev_k, prev_d = cur_k, cur_d
    return k, d, j


# ============================================================
# 选股条件
# ============================================================
def board_limit(code: str) -> float:
    """涨跌停幅度(%)。排除科创板后: 主板10 / 创业板20 / 北交所30"""
    if code.startswith(("300", "301")):
        return 20.0
    if code.startswith(("8", "920", "43")):  # 北交所
        return 30.0
    return 10.0  # 主板 60/00


def daily_changes(bars: list[dict]) -> list[float]:
    """每根K线相对前一日收盘的涨幅%(首根为 nan)"""
    out = [float("nan")] * len(bars)
    for i in range(1, len(bars)):
        prev = bars[i - 1]["close"]
        if prev:
            out[i] = (bars[i]["close"] / prev - 1) * 100
    return out


# 条件元数据: 供前端渲染勾选框 (单一数据源)
# 体系: A趋势结构 / B启动信号 / C买点状态 / D风控排除
COND_DEFS = [
    {"group": "一、趋势结构", "gid": "gA", "type": "and", "items": [
        {"id": "t1", "label": "均线多头排列 MA5>MA10>MA20>MA60", "hint": "严格从上到下多头结构"},
        {"id": "t2", "label": "MA20/MA60方向向上（今日>5日前）", "hint": "方向一致向上"},
        {"id": "t3", "label": "收盘价>MA20（站上生命线）", "hint": "不破中期趋势线"},
        {"id": "t4", "label": "长期趋势保护：收盘>MA250 或 MA60>MA250", "hint": "年线之上更可靠"},
    ]},
    {"group": "二、启动信号（满足其一）", "gid": "gB", "type": "or", "items": [
        {"id": "b1", "label": "近10日MA5上穿MA20 或 MA10上穿MA20", "hint": "金叉确认"},
        {"id": "b2", "label": "低位金叉：距250日最低涨幅<50%", "hint": "底部区域更可靠"},
        {"id": "b3", "label": "MA20连续5日走平后连续2日拐头向上", "hint": "拐头可替代金叉"},
    ]},
    {"group": "三、买点状态（满足其一）", "gid": "gC", "type": "or", "items": [
        {"id": "c1", "label": "低吸回踩：最低价触MA10/MA20±3%，收盘收回MA10上方，缩量", "hint": "回踩支撑不破"},
        {"id": "c2", "label": "放量突破：创20日新高，量>20日均量×1.5，涨幅≥3%阳线", "hint": "进攻型突破"},
    ]},
    {"group": "四、风控排除", "gid": "gD", "type": "and", "items": [
        {"id": "d1", "label": "乖离率<15%（(收盘-MA20)/MA20）", "hint": "避开末期追高"},
        {"id": "d2", "label": "排除空头结构（MA60非持续向下）", "hint": "一票否决"},
        {"id": "d3", "label": "排除ST/*ST", "hint": "退市风险"},
        {"id": "d4", "label": "排除停牌", "hint": "无法买入"},
        {"id": "d5", "label": "排除上市不足60日", "hint": "新股数据不稳"},
        {"id": "d6", "label": "排除当日一字板", "hint": "无法买入"},
        {"id": "d7", "label": "排除科创板(688)", "hint": "按需勾选"},
    ]},
]
# 全部条件ID (默认全勾选)
COND_ALL = [it["id"] for g in COND_DEFS for it in g["items"]]
# 各组包含的"评分叶子" (d3-d7 是剔除门, 不计入gD评分)
GROUP_LEAVES = {
    "gA": ["t1", "t2", "t3", "t4"],
    "gB": ["b1", "b2", "b3"],
    "gC": ["c1", "c2"],
    "gD": ["d1", "d2"],
}
# 剔除门条件 (勾选则剔除, 不参与评分)
GATE_CONDS = {"d3", "d4", "d5", "d6", "d7"}


def check_stock(row: dict, bars: list[dict], conds=None) -> dict | None:
    """
    对单只股票评估4大条件组(A趋势/B启动/C买点/D风控), 返回分组通过情况与指标快照。
    bars: 含当日的日K线(最后一条为当日), 建议至少260根(MA250)。
    剔除门(ST/科创板/停牌/上市不足60日/一字板)直接返回 None。
    其余始终返回结果, 含 score(0-4) 与各组布尔, 以便展示"接近满足"的标的。
    """
    if conds is None:
        conds = set(COND_ALL)
    else:
        conds = set(conds)
    code = row.get("code", "")
    name = row.get("name", "")
    if len(bars) < 21:
        return None
    # 剔除门 (可勾选): ST/*ST
    if "d3" in conds and ("ST" in name or "退" in name or "*ST" in name):
        return None
    # 剔除门: 科创板
    if "d7" in conds and code.startswith("688"):
        return None

    closes = [b["close"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    vols = [b["volume"] for b in bars]
    opens = [b["open"] for b in bars]
    chgs = daily_changes(bars)
    today = bars[-1]
    limit = board_limit(code)
    c = closes[-1]

    # 剔除门: 停牌(当日无成交)
    if "d4" in conds and today["volume"] == 0:
        return None
    # 剔除门: 上市不足60日
    if "d5" in conds and len(bars) < 60:
        return None
    # 剔除门: 当日一字板(开=收=高=低 或 振幅0且涨幅近涨停)
    if "d6" in conds:
        _ob, _cb, _hb, _lb = today["open"], today["close"], today["high"], today["low"]
        _chg = chgs[-1] if not math.isnan(chgs[-1]) else 0
        if (_hb == _lb) or (_ob == _cb == _hb == _lb) or (_chg >= limit * 0.99 and _hb == _lb):
            return None

    ma5 = sma(closes, 5)
    ma10 = sma(closes, 10)
    ma20 = sma(closes, 20)
    ma60 = sma(closes, 60)
    ma250 = sma(closes, 250) if len(closes) >= 250 else [float("nan")] * len(closes)
    # KDJ (保留作参考展示)
    _k, _d, _j = calc_kdj(highs, lows, closes)
    J_t = _j[-1] if not math.isnan(_j[-1]) else 0.0
    try:
        nmc = float(row.get("nmc", 0))
    except (TypeError, ValueError):
        nmc = 0.0
    try:
        today_amount = float(row.get("amount", 0))
    except (TypeError, ValueError):
        today_amount = today["volume"] * c

    # ---- A组: 趋势结构 叶子 ----
    # t1: 均线多头排列 MA5>MA10>MA20>MA60
    t1 = (not math.isnan(ma60[-1])) and ma5[-1] > ma10[-1] > ma20[-1] > ma60[-1]
    # t2: MA20/MA60方向向上 (今日>5日前)
    t2 = False
    if (not math.isnan(ma20[-1])) and len(ma20) >= 6 and (not math.isnan(ma20[-6])) \
       and (not math.isnan(ma60[-1])) and len(ma60) >= 6 and (not math.isnan(ma60[-6])):
        t2 = (ma20[-1] > ma20[-6]) and (ma60[-1] > ma60[-6])
    # t3: 收盘价>MA20 (站上生命线)
    t3 = (not math.isnan(ma20[-1])) and c > ma20[-1]
    # t4: 长期趋势保护 收盘>MA250 或 MA60>MA250
    m250 = ma250[-1] if not math.isnan(ma250[-1]) else float("nan")
    m60 = ma60[-1] if not math.isnan(ma60[-1]) else float("nan")
    t4 = False
    if not math.isnan(m250):
        t4 = (c > m250) or ((not math.isnan(m60)) and m60 > m250)
    else:
        # MA250不可用时, MA60方向向上视为长期趋势保护
        t4 = (not math.isnan(m60)) and len(ma60) >= 6 and (not math.isnan(ma60[-6])) and m60 > ma60[-6]

    # ---- B组: 启动信号 叶子 ----
    # b1: 近10日 MA5上穿MA20 或 MA10上穿MA20
    b1 = False
    for _i in range(max(1, len(closes) - 10), len(closes)):
        _p = _i - 1
        if _p < 0:
            continue
        if any(math.isnan(x) for x in (ma5[_i], ma20[_i], ma5[_p], ma20[_p])):
            continue
        # MA5上穿MA20
        if ma5[_p] <= ma20[_p] and ma5[_i] > ma20[_i]:
            b1 = True
            break
        # MA10上穿MA20
        if not math.isnan(ma10[_i]) and not math.isnan(ma10[_p]) and \
           not math.isnan(ma20[_p]) and ma10[_p] <= ma20[_p] and ma10[_i] > ma20[_i]:
            b1 = True
            break
    # b2: 低位金叉 距250日最低涨幅<50%
    b2 = False
    if b1:
        low250 = min(lows[-250:]) if len(lows) >= 250 else min(lows)
        gain_from_low = (c - low250) / low250 * 100 if low250 > 0 else 999
        b2 = gain_from_low < 50
    # b3: MA20连续5日走平后连续2日拐头向上
    b3 = False
    if len(ma20) >= 8 and not any(math.isnan(ma20[i]) for i in range(-8, 0)):
        flat = all(abs(ma20[i] - ma20[i - 1]) / ma20[i - 1] < 0.005 for i in range(-6, -1))
        turn = ma20[-1] > ma20[-2] > ma20[-3]
        b3 = flat and turn

    # ---- C组: 买点状态 叶子 ----
    # c1: 低吸回踩 (最低价触MA10/MA20±3%, 收盘收回MA10上方, 回踩日缩量)
    c1 = False
    if not math.isnan(ma10[-1]) and not math.isnan(ma20[-1]) and ma10[-1] > 0 and ma20[-1] > 0:
        near_ma = (abs(lows[-1] / ma10[-1] - 1) < 0.03) or (abs(lows[-1] / ma20[-1] - 1) < 0.03)
        close_above_ma10 = c > ma10[-1]
        vol_avg5 = sum(vols[-6:-1]) / 5 if len(vols) >= 6 else 0
        shrink = (vols[-1] < vol_avg5) if vol_avg5 > 0 else False
        c1 = near_ma and close_above_ma10 and shrink
    # c2: 放量突破 (创20日新高, 量>20日均量×1.5, 涨幅≥3%阳线)
    c2 = False
    if len(closes) >= 21:
        high20_close = max(closes[-21:-1])
        vol_avg20 = sum(vols[-21:-1]) / 20
        c2 = (c > high20_close) and (vol_avg20 > 0) and (vols[-1] > vol_avg20 * 1.5) and \
             (not math.isnan(chgs[-1]) and chgs[-1] >= 3) and (c > opens[-1])

    # ---- D组: 风控 叶子 ----
    # d1: 乖离率<15%
    bias = (c - ma20[-1]) / ma20[-1] * 100 if (not math.isnan(ma20[-1]) and ma20[-1] > 0) else 0
    d1 = bias < 15
    # d2: 排除空头结构 MA60非持续向下 (今日>=10日前)
    d2 = True
    if not math.isnan(ma60[-1]) and len(ma60) >= 11 and not math.isnan(ma60[-11]):
        d2 = ma60[-1] >= ma60[-11]

    leaf = {
        "t1": t1, "t2": t2, "t3": t3, "t4": t4,
        "b1": b1, "b2": b2, "b3": b3,
        "c1": c1, "c2": c2,
        "d1": d1, "d2": d2,
    }

    # ---- 各组通过情况 (尊重 conds) ----
    def and_group(gid):
        chk = [k for k in GROUP_LEAVES[gid] if k in conds]
        return all(leaf[k] for k in chk) if chk else True

    def or_group(gid):
        chk = [k for k in GROUP_LEAVES[gid] if k in conds]
        return any(leaf[k] for k in chk) if chk else True

    groups = {"gA": and_group("gA"), "gB": or_group("gB"),
              "gC": or_group("gC"), "gD": and_group("gD")}
    active = {gid: any(k in conds for k in GROUP_LEAVES[gid]) for gid in GROUP_LEAVES}
    n_active = sum(active.values())
    score = sum(groups[g] for g in groups if active[g])

    # 命中标签 (展示用, 仅展示已勾选且通过的叶子)
    hits = []
    if active["gA"]:
        if "t1" in conds and leaf["t1"]:
            hits.append("多头排列")
        if "t2" in conds and leaf["t2"]:
            hits.append("方向向上")
        if "t3" in conds and leaf["t3"]:
            hits.append("站上MA20")
        if "t4" in conds and leaf["t4"]:
            hits.append("年线保护")
    if active["gB"]:
        if "b1" in conds and leaf["b1"]:
            hits.append("金叉确认")
        if "b2" in conds and leaf["b2"]:
            hits.append("低位金叉")
        if "b3" in conds and leaf["b3"]:
            hits.append("MA20拐头")
    if active["gC"]:
        if "c1" in conds and leaf["c1"]:
            hits.append("低吸回踩")
        if "c2" in conds and leaf["c2"]:
            hits.append("放量突破")
    if active["gD"]:
        if "d1" in conds and leaf["d1"]:
            hits.append("乖离安全")

    est = estimate_gains(bars, ma5, ma10, ma20, _j)
    est["tags"] = hits + [t for t in est["tags"] if t not in hits]

    return {
        "code": code,
        "name": name,
        "symbol": row.get("symbol", ""),
        "industry": get_industry(code),
        "concept": get_concepts(code),
        "price": round(c, 2),
        "change_pct": round(float(row.get("changepercent", 0) or 0), 2),
        "amount_yi": round(today_amount / 1e8, 2),
        "float_mv_yi": round(nmc / 10000, 2),
        "ma5": round(ma5[-1], 2),
        "ma10": round(ma10[-1], 2),
        "ma20": round(ma20[-1], 2),
        "ma60": round(ma60[-1], 2) if not math.isnan(ma60[-1]) else 0,
        "ma250": round(ma250[-1], 2) if not math.isnan(ma250[-1]) else 0,
        "bias": round(bias, 2),
        "kdj_j": round(J_t, 2),
        "score": score,
        "n_active": n_active,
        "groups": groups,
        "active": active,
        "est_5d": est["d5"],
        "est_10d": est["d10"],
        "est_20d": est["d20"],
        "tags": est["tags"],
    }


def estimate_gains(bars, ma5, ma10, ma20, j) -> dict:
    """
    近5/10/20日涨幅预估 (透明启发式, 非投资建议)。
    综合近期动量 + 均线趋势 + KDJ信号 + 位置, 向前线性外推并加阻尼。
    """
    closes = [b["close"] for b in bars]
    # 近5日动量(日化)
    if len(closes) >= 6 and closes[-6] > 0:
        r5 = closes[-1] / closes[-6] - 1
        daily_mom = (1 + r5) ** (1 / 5) - 1
    else:
        daily_mom = 0.0
    daily_mom = max(min(daily_mom, 0.04), -0.04)  # 限制极端

    # 信号分: [-1, 1]
    sig = 0.0
    reasons, tags = [], []
    # 均线多头
    if ma5[-1] > ma10[-1] > ma20[-1]:
        sig += 0.3
        tags.append("均线多头")
    elif ma5[-1] > ma10[-1]:
        sig += 0.15
        tags.append("短多")
    # KDJ
    J_t = j[-1]
    if not math.isnan(J_t):
        if J_t < 20 and j[-1] > j[-2]:
            sig += 0.35
            tags.append("KDJ低位拐头")
        elif j[-1] > j[-2] and J_t < 60:
            sig += 0.2
            tags.append("KDJ上行")
    # 位置: 距20日高点的空间(超跌反弹预期)
    if len(closes) >= 20:
        hi = max(closes[-20:])
        room = (hi - closes[-1]) / closes[-1]
        if room > 0.05:
            sig += min(0.25, room * 0.5)
            tags.append("超跌反弹")

    sig = max(min(sig, 1.0), -1.0)
    # 预估上限基于"距20日高点的空间"(均值回归天花板), 更贴近现实:
    # 已在高位的强势股上行空间有限, 回调充分的标的空间更大。
    if len(closes) >= 20:
        hi20 = max(closes[-20:])
        room = (hi20 - closes[-1]) / closes[-1]  # 距高点的上行空间(>=0)
    else:
        room = 0.10
    cap5 = min(0.15, room * 0.50 + 0.03)
    cap10 = min(0.25, room * 0.80 + 0.05)
    cap20 = min(0.40, room * 1.20 + 0.08)
    # 超买(J>80)追高风险: 预估打折扣
    if not math.isnan(J_t) and J_t > 80:
        cap5 *= 0.5
        cap10 *= 0.5
        cap20 *= 0.5
        tags.append("超买谨慎")
    # 线性外推 + 信号加权 + 阻尼
    d5 = daily_mom * 5 * (1 + 0.5 * sig) * 0.8
    d10 = daily_mom * 10 * (1 + 0.4 * sig) * 0.7
    d20 = daily_mom * 20 * (1 + 0.3 * sig) * 0.6
    # 按个股空间设定上限/下限
    d5 = max(min(d5, cap5), -cap5 * 0.6)
    d10 = max(min(d10, cap10), -cap10 * 0.6)
    d20 = max(min(d20, cap20), -cap20 * 0.6)

    if daily_mom > 0:
        reasons.append("近期动量向上")
    elif daily_mom < 0:
        reasons.append("近期动量偏弱")
    return {
        "d5": round(d5 * 100, 2),
        "d10": round(d10 * 100, 2),
        "d20": round(d20 * 100, 2),
        "reasons": reasons,
        "tags": tags,
    }


# ============================================================
# 选股编排
# ============================================================
def run_screen(conds=None) -> dict:
    if conds is None:
        conds = set(COND_ALL)
    else:
        conds = set(conds)
    t0 = time.time()
    # 1. 全市场快照
    spot = fetch_spot_all()
    # 2. 预过滤: 按 conds 剔除门, 减少K线拉取量
    candidates = []
    for r in spot:
        code = r.get("code", "")
        name = r.get("name", "")
        if "d7" in conds and code.startswith("688"):
            continue
        if "d3" in conds and ("ST" in name or "退" in name or "*ST" in name):
            continue
        candidates.append(r)

    # 3. 构建行业/概念映射
    _build_board_maps()

    # 4. 并发拉取K线并筛选 (MA250需300根)
    def work(r):
        try:
            bars = fetch_kline(r.get("symbol"), datalen=300)
            if not bars:
                return None
            return check_stock(r, bars, conds)
        except Exception:  # noqa: BLE001
            return None

    # 激活组与必选/可选分组
    # gA(趋势结构) gD(风控) 为必选组: 激活时必须通过, 否则不纳入
    # gB(启动信号) gC(买点状态) 为可选组: 允许差1组作为"接近满足"
    MANDATORY = {"gA", "gD"}
    active_g = {gid: any(k in conds for k in GROUP_LEAVES[gid]) for gid in GROUP_LEAVES}
    n_active = sum(active_g.values())
    flex_active = {gid: active_g[gid] and gid not in MANDATORY for gid in GROUP_LEAVES}
    n_flex = sum(flex_active.values())
    flex_threshold = max(0, n_flex - 1) if n_flex > 0 else 0

    results: list[dict] = []
    futs = [POOL.submit(work, r) for r in candidates]
    for f in as_completed(futs):
        res = f.result()
        if not res:
            continue
        g = res["groups"]
        # 必选组: 激活就必须通过
        if any(active_g[m] and not g[m] for m in MANDATORY):
            continue
        # 可选组: 通过数 >= flex_threshold (允许差1组)
        flex_pass = sum(1 for gid in GROUP_LEAVES if flex_active[gid] and g[gid])
        if flex_pass < flex_threshold:
            continue
        results.append(res)

    # 完全命中(全部激活组通过) 与 接近满足(可选组差1) 分开
    exact = [r for r in results if n_active > 0 and r["score"] == n_active]
    near = [r for r in results if not (n_active > 0 and r["score"] == n_active)]
    exact.sort(key=lambda x: x["est_20d"], reverse=True)
    near.sort(key=lambda x: (x["score"], x["est_20d"]), reverse=True)
    near = near[:60]  # 控制前端载荷

    return {
        "updated": bj_now(),
        "elapsed_sec": round(time.time() - t0, 1),
        "universe": len(spot),
        "candidates": len(candidates),
        "matched": len(exact),
        "near_count": len(near),
        "exact": exact,
        "stocks": near,  # 接近满足列表
        "conds": sorted(conds),
        "n_active": n_active,
        "active": active_g,
    }


# ============================================================
# FastAPI
# ============================================================
app = FastAPI(title="A股选股系统")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SCREEN_TTL = 300  # 选股结果缓存 5 分钟
# 后台筛选状态: data/缓存时间/是否运行中/上次错误/进度/上次使用的conds
_state = {"data": None, "ts": 0.0, "running": False, "error": None,
          "progress": "", "last_conds": set(COND_ALL)}
_screen_lock = threading.Lock()


def _run_screen_thread(conds=None):
    """在后台线程执行筛选, 写入 _state。同一时刻仅一个在跑。"""
    with _screen_lock:
        if _state["running"]:
            return
        _state["running"] = True
    c = set(conds) if conds is not None else set(_state.get("last_conds", COND_ALL))
    try:
        _state["progress"] = "拉取全市场实时行情…"
        data = run_screen(c)
        with _screen_lock:
            _state["data"] = data
            _state["ts"] = time.time()
            _state["error"] = None
            _state["progress"] = ""
            _state["last_conds"] = c
    except Exception as e:  # noqa: BLE001
        with _screen_lock:
            _state["error"] = str(e)
            _state["progress"] = ""
    finally:
        with _screen_lock:
            _state["running"] = False


def _ensure_screen():
    """缓存缺失或过期则用上次conds启动后台筛选(非阻塞)。"""
    if _state["running"]:
        return
    if _state["data"] and (time.time() - _state["ts"] < SCREEN_TTL):
        return
    threading.Thread(target=_run_screen_thread, daemon=True).start()


@app.get("/api/conds")
def api_conds():
    """返回条件定义(单一数据源) + 全部ID(默认全选)。"""
    return {"defs": COND_DEFS, "all": COND_ALL}


@app.get("/api/screen")
def api_screen():
    """始终即时返回: 有缓存就返回缓存, 否则返回 running 状态供前端轮询。"""
    _ensure_screen()
    data = _state["data"]
    with _screen_lock:
        running = _state["running"]
        err = _state["error"]
        progress = _state["progress"]
        last_conds = list(_state.get("last_conds", COND_ALL))
    if data:
        out = dict(data)
        out["running"] = running
        out["cached"] = True
        out["error"] = err
        out["conds"] = last_conds
        return out
    # 尚无缓存: 筛选进行中或出错
    return JSONResponse({
        "running": running,
        "cached": False,
        "error": err,
        "progress": progress or "正在启动筛选…",
        "conds": last_conds,
        "stocks": [], "exact": [], "matched": 0, "near_count": 0,
        "updated": "", "elapsed_sec": 0, "universe": 0, "candidates": 0,
        "n_active": 0,
    })


@app.post("/api/screen/run")
def api_run(payload: dict):
    """用前端勾选的 conds 启动后台筛选, 立即返回。前端轮询 /api/screen。"""
    conds = payload.get("conds") if isinstance(payload, dict) else None
    if not isinstance(conds, list):
        return JSONResponse({"ok": False, "msg": "conds 必须为数组"}, status_code=400)
    valid = set(COND_ALL)
    conds = [c for c in conds if c in valid]
    if not conds:
        return JSONResponse({"ok": False, "msg": "至少勾选一个条件"}, status_code=400)
    with _screen_lock:
        already = _state["running"]
        _state["last_conds"] = set(conds)  # 记录待用conds
        _state["ts"] = 0.0  # 标记缓存过期
    if not already:
        threading.Thread(target=_run_screen_thread, args=(set(conds),), daemon=True).start()
    return {"ok": True, "started": True, "running": _state["running"], "conds": conds}


@app.post("/api/screen/refresh")
def api_refresh():
    """用上次conds触发后台重新筛选(忽略缓存), 立即返回。"""
    if not _state["running"]:
        with _screen_lock:
            _state["ts"] = 0.0
        threading.Thread(target=_run_screen_thread, daemon=True).start()
    with _screen_lock:
        running = _state["running"]
    return {"started": True, "running": running}


@app.get("/api/health")
def health():
    return {"ok": True, "time": bj_now(),
            "running": _state["running"], "cached": _state["data"] is not None}


# ============================================================
# 本地缓存管理 API
# ============================================================
@app.get("/api/cache_info")
def cache_info():
    """返回本地文件缓存信息 (缓存日期/K线数/行情快照大小/生成时间)"""
    import glob
    cache_date = _cache_date_for_fetch()
    spot_path = _spot_cache_path(cache_date)
    kline_dir = _kline_cache_dir(cache_date)
    # 行情快照
    spot_size = 0
    spot_mtime = None
    if os.path.exists(spot_path):
        spot_size = os.path.getsize(spot_path)
        spot_mtime = datetime.fromtimestamp(os.path.getmtime(spot_path), _BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")
    # K线
    kline_count = 0
    kline_size = 0
    kline_mtime = None
    if os.path.isdir(kline_dir):
        files = glob.glob(os.path.join(kline_dir, "*.json"))
        kline_count = len(files)
        for fp in files:
            kline_size += os.path.getsize(fp)
        if files:
            latest_mt = max(os.path.getmtime(fp) for fp in files)
            kline_mtime = datetime.fromtimestamp(latest_mt, _BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "cache_date": cache_date,
        "spot_size_mb": round(spot_size / 1024 / 1024, 2),
        "spot_mtime": spot_mtime,
        "kline_count": kline_count,
        "kline_size_mb": round(kline_size / 1024 / 1024, 2),
        "kline_mtime": kline_mtime,
        "total_mb": round((spot_size + kline_size) / 1024 / 1024, 2),
    }


@app.post("/api/cache_refresh")
def cache_refresh():
    """清空本地文件缓存(当日), 触发重新拉取。返回新缓存任务状态。"""
    import shutil
    cache_date = _cache_date_for_fetch()
    # 删除当日行情快照
    spot_path = _spot_cache_path(cache_date)
    if os.path.exists(spot_path):
        try:
            os.remove(spot_path)
        except OSError:
            pass
    # 删除当日K线目录
    kline_dir = _kline_cache_dir(cache_date)
    if os.path.isdir(kline_dir):
        try:
            shutil.rmtree(kline_dir)
        except OSError:
            pass
    # 标记内存筛选结果过期, 触发重新筛选(会重新拉取并缓存)
    with _screen_lock:
        _state["ts"] = 0.0
    if not _state["running"]:
        threading.Thread(target=_run_screen_thread, daemon=True).start()
    return {"ok": True, "cleared": cache_date, "running": _state["running"]}


# ============================================================
# 个股分析模块
# ============================================================
_stock_search_cache = {"built_at": 0.0, "items": []}
_SEARCH_TTL = 3600  # 1小时


def _to_symbol(code: str) -> str:
    """代码 -> 新浪symbol"""
    code = code.strip()
    if code.startswith(("sh", "sz")):
        return code
    if code.startswith(("6", "5", "9", "11", "13")):
        return "sh" + code
    return "sz" + code


def fetch_stock_profile(code: str) -> dict:
    """从新浪公司简介页获取主营业务等信息"""
    import re as _re
    url = f"http://money.finance.sina.com.cn/corp/go.php/vCI_CorpInfo/stockid/{code}.phtml"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.encoding = "gb2312"
        text = r.text
        result = {}
        for kw in ["主营业务", "所属行业", "经营范围", "上市日期"]:
            m = _re.search(kw + r"[：:]?\s*</td>\s*<td[^>]*>(.*?)</td>", text, _re.S)
            if m:
                val = _re.sub(r"<[^>]+>", "", m.group(1)).strip()
                if val:
                    result[kw] = val
        return result
    except Exception:  # noqa: BLE001
        return {}


def _build_search_index():
    """构建股票搜索索引(含拼音首字母), 缓存1小时"""
    now = time.time()
    if now - _stock_search_cache["built_at"] < _SEARCH_TTL and _stock_search_cache["items"]:
        return
    try:
        from pypinyin import lazy_pinyin
    except ImportError:
        lazy_pinyin = None
    spot = fetch_spot_all()
    items = []
    for r in spot:
        code = r.get("code", "")
        name = r.get("name", "")
        if not code or not name:
            continue
        py_abbr = ""
        py_full = ""
        if lazy_pinyin:
            try:
                py = lazy_pinyin(name)
                py_abbr = "".join(p[0] for p in py if p)
                py_full = "".join(py)
            except Exception:  # noqa: BLE001
                pass
        items.append({
            "code": code, "name": name, "symbol": r.get("symbol", ""),
            "py_abbr": py_abbr.lower(), "py_full": py_full.lower(),
        })
    _stock_search_cache["items"] = items
    _stock_search_cache["built_at"] = now


@app.get("/api/stock/search")
def api_stock_search(q: str = ""):
    """按代码/名称/拼音首字母联想搜索, 返回最多10条"""
    _build_search_index()
    q = q.strip().lower()
    if not q:
        return {"items": []}
    items = _stock_search_cache["items"]
    code_match = [it for it in items if q in it["code"].lower()]
    name_match = [it for it in items if q in it["name"]]
    py_match = [it for it in items if q in it["py_abbr"] or q in it["py_full"]]
    seen = set()
    result = []
    for it in code_match[:5] + name_match[:5] + py_match[:5]:
        if it["code"] not in seen:
            seen.add(it["code"])
            result.append({"code": it["code"], "name": it["name"], "symbol": it["symbol"]})
        if len(result) >= 10:
            break
    return {"items": result}


@app.get("/api/stock/analyze")
def api_stock_analyze(code: str = ""):
    """个股深度分析: 基本信息 + 技术指标 + 量价关系"""
    code = code.strip()
    if not code:
        return JSONResponse({"error": "code is required"}, status_code=400)
    symbol = _to_symbol(code)
    # 1. 实时行情
    spot = _get(SINA_HQ, {"page": 1, "num": 1, "node": "hs_a"})
    # 上面方式取不到单只, 改用直接搜索全量缓存
    _build_search_index()
    info = None
    for it in _stock_search_cache["items"]:
        if it["code"] == code or it["symbol"] == symbol:
            info = it
            break
    # 如果缓存里找不到, 尝试拉K线判断是否存在
    bars = fetch_kline(symbol, datalen=60)
    if not bars:
        return JSONResponse({"error": f"找不到股票 {code} 或无K线数据"}, status_code=404)

    # 构建行业/概念映射
    _build_board_maps()
    closes = [b["close"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    vols = [b["volume"] for b in bars]
    chgs = daily_changes(bars)

    # 日线指标
    ma5 = sma(closes, 5)
    ma10 = sma(closes, 10)
    ma20 = sma(closes, 20)
    k, d, j = calc_kdj(highs, lows, closes)
    J_t = j[-1] if not math.isnan(j[-1]) else 0
    K_t = k[-1] if not math.isnan(k[-1]) else 0
    D_t = d[-1] if not math.isnan(d[-1]) else 0

    # 周线指标
    weekly_highs, weekly_lows, weekly_closes = [], [], []
    for i in range(0, len(closes), 5):
        j_end = min(i + 5, len(closes))
        weekly_highs.append(max(highs[i:j_end]))
        weekly_lows.append(min(lows[i:j_end]))
        weekly_closes.append(closes[j_end - 1])
    ma5w = sma(weekly_closes, 5) if len(weekly_closes) >= 2 else [float("nan")]
    ma10w = sma(weekly_closes, 10) if len(weekly_closes) >= 2 else [float("nan")]
    wk, wd, wj = calc_kdj(weekly_highs, weekly_lows, weekly_closes)
    wJ_t = wj[-1] if wj and not math.isnan(wj[-1]) else 0

    # 量价分析
    vol_avg5 = sum(vols[-5:]) / 5 if len(vols) >= 5 else 0
    vol_avg10 = sum(vols[-10:]) / 10 if len(vols) >= 10 else 0
    vol_avg20 = sum(vols[-20:]) / 20 if len(vols) >= 20 else 0
    last_amt = bars[-1]["close"] * bars[-1]["volume"]
    yest_amt = bars[-2]["close"] * bars[-2]["volume"] if len(bars) >= 2 else 0

    # 涨跌幅统计
    chg_1d = chgs[-1] if chgs and not math.isnan(chgs[-1]) else 0
    chg_5d = (closes[-1] / closes[-6] - 1) * 100 if len(closes) >= 6 else 0
    chg_10d = (closes[-1] / closes[-11] - 1) * 100 if len(closes) >= 11 else 0
    chg_20d = (closes[-1] / closes[-21] - 1) * 100 if len(closes) >= 21 else 0
    high20 = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    low20 = min(lows[-20:]) if len(lows) >= 20 else min(lows)
    pos_20d = (closes[-1] - low20) / (high20 - low20) * 100 if high20 > low20 else 50

    # 技术分析文字描述
    analysis = []
    # 均线分析
    if not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) and not math.isnan(ma20[-1]):
        if ma5[-1] > ma10[-1] > ma20[-1]:
            analysis.append({"item": "均线趋势", "desc": "5/10/20日均线多头排列，中期趋势向上", "status": "看多"})
        elif ma5[-1] < ma10[-1] < ma20[-1]:
            analysis.append({"item": "均线趋势", "desc": "5/10/20日均线空头排列，中期趋势向下", "status": "看空"})
        elif ma5[-1] > ma10[-1]:
            analysis.append({"item": "均线趋势", "desc": "短期均线(5日)在10日上方，短期偏强", "status": "偏多"})
        else:
            analysis.append({"item": "均线趋势", "desc": "短期均线(5日)在10日下方，短期偏弱", "status": "偏空"})
        if not math.isnan(ma5[-3]) and ma5[-1] > ma5[-3]:
            analysis.append({"item": "均线方向", "desc": "5日均线向上发散", "status": "看多"})
        elif not math.isnan(ma5[-3]) and ma5[-1] < ma5[-3]:
            analysis.append({"item": "均线方向", "desc": "5日均线向下发散", "status": "看空"})
    # KDJ分析
    if not math.isnan(j[-1]):
        if J_t < 20:
            analysis.append({"item": "KDJ位置", "desc": f"J值={J_t:.1f}，处于超卖区域，有反弹需求", "status": "看多"})
        elif J_t > 80:
            analysis.append({"item": "KDJ位置", "desc": f"J值={J_t:.1f}，处于超买区域，追高风险大", "status": "看空"})
        else:
            analysis.append({"item": "KDJ位置", "desc": f"J值={J_t:.1f}，处于中性区间", "status": "中性"})
        if not math.isnan(j[-2]) and J_t > j[-2]:
            analysis.append({"item": "KDJ方向", "desc": "J值上行，动能增强", "status": "看多"})
        elif not math.isnan(j[-2]) and J_t < j[-2]:
            analysis.append({"item": "KDJ方向", "desc": "J值下行，动能减弱", "status": "看空"})
        if not math.isnan(k[-1]) and not math.isnan(d[-1]):
            if J_t > K_t > D_t and J_t > j[-2]:
                analysis.append({"item": "KDJ金叉", "desc": "J>K>D且上行，金叉形态", "status": "看多"})
            elif J_t < K_t < D_t and J_t < j[-2]:
                analysis.append({"item": "KDJ死叉", "desc": "J<K<D且下行，死叉形态", "status": "看空"})
    # 量价分析
    if vol_avg5 > 0 and vol_avg10 > 0:
        vol_ratio = vol_avg5 / vol_avg10
        if vol_ratio > 1.3 and chg_1d > 0:
            analysis.append({"item": "量价关系", "desc": f"近5日均量为10日均量{vol_ratio:.1f}倍，放量上涨，资金活跃", "status": "看多"})
        elif vol_ratio < 0.7 and chg_1d < 0:
            analysis.append({"item": "量价关系", "desc": f"近5日均量仅为10日均量{vol_ratio:.1f}倍，缩量下跌，抛压减弱", "status": "偏多"})
        elif vol_ratio > 1.3 and chg_1d < 0:
            analysis.append({"item": "量价关系", "desc": f"放量下跌({vol_ratio:.1f}倍)，资金出逃信号", "status": "看空"})
        elif vol_ratio < 0.7:
            analysis.append({"item": "量价关系", "desc": f"缩量整理({vol_ratio:.1f}倍)，观望情绪浓", "status": "中性"})
        else:
            analysis.append({"item": "量价关系", "desc": f"量价配合正常({vol_ratio:.1f}倍)", "status": "中性"})
    # 位置分析
    analysis.append({"item": "区间位置", "desc": f"当前价位处于近20日区间的{pos_20d:.0f}%位置(0%=最低,100%=最高)", "status": "偏多" if pos_20d < 30 else ("偏空" if pos_20d > 80 else "中性")})
    # 周线分析
    if not math.isnan(ma5w[-1]) and not math.isnan(ma10w[-1]):
        if ma5w[-1] > ma10w[-1]:
            analysis.append({"item": "周线均线", "desc": f"5周均线({ma5w[-1]:.2f})在10周均线({ma10w[-1]:.2f})上方，中期趋势向上", "status": "看多"})
        else:
            analysis.append({"item": "周线均线", "desc": f"5周均线({ma5w[-1]:.2f})在10周均线({ma10w[-1]:.2f})下方，中期趋势向下", "status": "看空"})
    if wJ_t > 0:
        if wJ_t < 20:
            analysis.append({"item": "周线KDJ", "desc": f"周J值={wJ_t:.1f}，低位区域", "status": "看多"})
        elif wJ_t > 90:
            analysis.append({"item": "周线KDJ", "desc": f"周J值={wJ_t:.1f}，高位超买区域", "status": "看空"})
        else:
            analysis.append({"item": "周线KDJ", "desc": f"周J值={wJ_t:.1f}，中性区域", "status": "中性"})

    name = info["name"] if info else code
    limit_pct = board_limit(code)
    # 公司简介 (主营业务等)
    profile = fetch_stock_profile(code)

    return {
        "code": code,
        "name": name,
        "symbol": symbol,
        "industry": get_industry(code),
        "concept": get_concepts(code),
        "main_business": profile.get("主营业务", "—"),
        "limit_pct": limit_pct,
        "price": round(closes[-1], 2),
        "change_pct": round(chg_1d, 2),
        "change_5d": round(chg_5d, 2),
        "change_10d": round(chg_10d, 2),
        "change_20d": round(chg_20d, 2),
        "amount_yi": round(last_amt / 1e8, 2),
        "yest_amount_yi": round(yest_amt / 1e8, 2),
        "ma5": round(ma5[-1], 2) if not math.isnan(ma5[-1]) else 0,
        "ma10": round(ma10[-1], 2) if not math.isnan(ma10[-1]) else 0,
        "ma20": round(ma20[-1], 2) if not math.isnan(ma20[-1]) else 0,
        "ma5w": round(ma5w[-1], 2) if not math.isnan(ma5w[-1]) else 0,
        "ma10w": round(ma10w[-1], 2) if not math.isnan(ma10w[-1]) else 0,
        "kdj_j": round(J_t, 2),
        "kdj_k": round(K_t, 2),
        "kdj_d": round(D_t, 2),
        "kdj_jw": round(wJ_t, 2),
        "vol_avg5": round(vol_avg5, 0),
        "vol_avg10": round(vol_avg10, 0),
        "vol_avg20": round(vol_avg20, 0),
        "high20": round(high20, 2),
        "low20": round(low20, 2),
        "pos_20d": round(pos_20d, 1),
        "bars": [{"day": b["day"], "close": b["close"], "open": b["open"],
                  "high": b["high"], "low": b["low"], "volume": b["volume"],
                  "chg": round(c, 2) if not math.isnan(c) else 0}
                 for b, c in zip(bars[-30:], chgs[-30:])],
        "analysis": analysis,
        "updated": bj_now(),
    }


# ============================================================
# 均线形态筛选模块
# ============================================================
MA_PATTERNS = ["多头排列", "多头排列向上发散", "粘合向上突破", "空头排列向下发散", "粘合向下突破"]

_ma_state = {"data": None, "running": False, "error": None, "progress": "",
             "ts": 0.0, "lock": threading.Lock()}
_MA_CACHE_TTL = 300  # 5分钟


def classify_ma_pattern(bars: list[dict]) -> str | None:
    """分类均线形态, 返回形态名或None"""
    if len(bars) < 70:
        return None
    closes = [b["close"] for b in bars]
    ma5 = sma(closes, 5)
    ma10 = sma(closes, 10)
    ma20 = sma(closes, 20)
    ma60 = sma(closes, 60)
    m5, m10, m20, m60 = ma5[-1], ma10[-1], ma20[-1], ma60[-1]
    if any(math.isnan(x) for x in [m5, m10, m20, m60]):
        return None
    m5p, m10p, m20p, m60p = ma5[-2], ma10[-2], ma20[-2], ma60[-2]
    if any(math.isnan(x) for x in [m5p, m10p, m20p, m60p]):
        return None
    c = closes[-1]
    # 粘合判断: 3天前三线间距占价格比 < 1.5%
    past_spread = 999.0
    if len(ma20) >= 4 and not math.isnan(ma20[-4]):
        pv = [ma5[-4], ma10[-4], ma20[-4]]
        if not any(math.isnan(v) for v in pv):
            past_spread = (max(pv) - min(pv)) / c * 100

    # 1. 多头排列 (模板A - 稳健型):
    #    a) MA5>MA10>MA20>MA60  b) 四线今日>5日前  c) 收盘>=MA10  d) 持续>=5日
    if m5 > m10 > m20 > m60:
        dirs_ok = (len(ma5) >= 6 and len(ma10) >= 6 and
                   len(ma20) >= 6 and len(ma60) >= 6)
        if dirs_ok:
            v5d = [ma5[-6], ma10[-6], ma20[-6], ma60[-6]]
            if not any(math.isnan(v) for v in v5d):
                if m5 > ma5[-6] and m10 > ma10[-6] and m20 > ma20[-6] and m60 > ma60[-6]:
                    if c >= m10:
                        persistent = True
                        for i in range(-5, 0):
                            if (abs(i) > len(ma5) - 1 or abs(i) > len(ma10) - 1 or
                                    abs(i) > len(ma20) - 1 or abs(i) > len(ma60) - 1):
                                persistent = False
                                break
                            if not (ma5[i] > ma10[i] > ma20[i] > ma60[i]):
                                persistent = False
                                break
                        if persistent:
                            return "多头排列"

    # 2. 粘合向上突破: 过去粘合, 现在多头排列
    if past_spread < 1.5 and m5 > m10 > m20:
        return "粘合向上突破"
    # 3. 粘合向下突破: 过去粘合, 现在空头排列
    if past_spread < 1.5 and m5 < m10 < m20:
        return "粘合向下突破"
    # 4. 多头排列向上发散 (宽松版, 不含MA60)
    if m5 > m10 > m20 and m5 > m5p and m10 > m10p:
        return "多头排列向上发散"
    # 5. 空头排列向下发散: 空头排列 + 均线下行
    if m5 < m10 < m20 and m5 < m5p and m10 < m10p:
        return "空头排列向下发散"
    return None


def _run_ma_screen_thread():
    """后台均线形态筛选线程"""
    with _ma_state["lock"]:
        if _ma_state["running"]:
            return
        _ma_state["running"] = True
        _ma_state["error"] = None
        _ma_state["progress"] = "拉取全市场行情…"
    t0 = time.time()
    try:
        spot = fetch_spot_all()
        _build_board_maps()
        # 预过滤: 排除ST/科创板/北交所, 成交额>1亿
        cands = []
        for r in spot:
            code = r.get("code", "")
            name = r.get("name", "")
            if not code or not name:
                continue
            if "ST" in name or code.startswith(("688", "8", "4")):
                continue
            try:
                amt = float(r.get("amount", 0))
            except (TypeError, ValueError):
                amt = 0
            if amt < 1e8:
                continue
            cands.append({"code": code, "name": name, "row": r})
        _ma_state["progress"] = f"预筛 {len(cands)} 只, 并发拉取K线中…"
        results = {p: [] for p in MA_PATTERNS}
        done = [0]
        total = len(cands)

        def process_stock(cand):
            symbol = _to_symbol(cand["code"])
            bars = fetch_kline(symbol, datalen=80)
            if not bars or len(bars) < 70:
                return None
            pat = classify_ma_pattern(bars)
            if not pat:
                return None
            closes = [b["close"] for b in bars]
            ma5 = sma(closes, 5)
            ma10 = sma(closes, 10)
            ma20 = sma(closes, 20)
            ma60 = sma(closes, 60)
            chg = (closes[-1] / closes[-2] - 1) * 100 if len(closes) >= 2 else 0
            return pat, {
                "code": cand["code"], "name": cand["name"],
                "price": round(closes[-1], 2),
                "change_pct": round(chg, 2),
                "ma5": round(ma5[-1], 2) if not math.isnan(ma5[-1]) else 0,
                "ma10": round(ma10[-1], 2) if not math.isnan(ma10[-1]) else 0,
                "ma20": round(ma20[-1], 2) if not math.isnan(ma20[-1]) else 0,
                "ma60": round(ma60[-1], 2) if not math.isnan(ma60[-1]) else 0,
                "amount_yi": round(float(cand["row"].get("amount", 0)) / 1e8, 2),
                "industry": get_industry(cand["code"]),
                "concept": get_concepts(cand["code"]),
            }
        futs = [POOL.submit(process_stock, c) for c in cands]
        for f in as_completed(futs):
            try:
                r = f.result()
                if r:
                    pat, item = r
                    results[pat].append(item)
            except Exception:  # noqa: BLE001
                pass
            done[0] += 1
            if done[0] % 100 == 0:
                _ma_state["progress"] = f"已处理 {done[0]}/{total}…"
        for p in MA_PATTERNS:
            results[p].sort(key=lambda x: -x["amount_yi"])
        out = {
            "patterns": {p: results[p] for p in MA_PATTERNS},
            "counts": {p: len(results[p]) for p in MA_PATTERNS},
            "total_stocks": total,
            "elapsed_sec": round(time.time() - t0, 1),
            "updated": bj_now(),
        }
        with _ma_state["lock"]:
            _ma_state["data"] = out
            _ma_state["running"] = False
            _ma_state["ts"] = time.time()
    except Exception as e:  # noqa: BLE001
        with _ma_state["lock"]:
            _ma_state["error"] = str(e)
            _ma_state["running"] = False


def _ensure_ma_screen():
    """首次访问时自动触发筛选(缓存过期则重新筛选)"""
    now = time.time()
    with _ma_state["lock"]:
        age = now - _ma_state["ts"]
        if _ma_state["data"] and age < _MA_CACHE_TTL:
            return
        if _ma_state["running"]:
            return
    threading.Thread(target=_run_ma_screen_thread, daemon=True).start()


@app.post("/api/ma-screen/run")
def api_ma_screen_run():
    """触发均线形态筛选"""
    if not _ma_state["running"]:
        with _ma_state["lock"]:
            _ma_state["ts"] = 0.0
        threading.Thread(target=_run_ma_screen_thread, daemon=True).start()
    return {"started": True, "running": _ma_state["running"]}


@app.get("/api/ma-screen")
def api_ma_screen():
    """获取均线形态筛选结果"""
    _ensure_ma_screen()
    with _ma_state["lock"]:
        running = _ma_state["running"]
        err = _ma_state["error"]
        progress = _ma_state["progress"]
    data = _ma_state["data"]
    if data:
        out = dict(data)
        out["running"] = running
        out["error"] = err
        return out
    return JSONResponse({"running": running, "error": err, "progress": progress,
                         "patterns": {}, "counts": {}})


@app.on_event("startup")
def _startup():
    # 服务启动即预热缓存(默认全条件), 避免首次请求等待
    threading.Thread(target=_run_screen_thread, daemon=True).start()


# 静态前端
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")


if __name__ == "__main__":
    import uvicorn
    # 云平台(Render/Railway)通过 PORT 环境变量指定端口, 默认 8000
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
