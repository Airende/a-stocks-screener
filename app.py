# -*- coding: utf-8 -*-
"""
ARD选股系统 - 后端
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
from pydantic import BaseModel

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
_board_cache = {"built_at": 0.0, "industry": {}, "industry2": {}, "industry3": {}, "concept": {}}
# ============================================================
# 板块分类: 对齐"开盘啦"口径的修正与增强
# 策略: 细分概念/行业(第1位) + 大行业/大概念(第2位)
# ============================================================
# 1. 个股"第一身份"白名单: 头部权重股的细分概念完全对齐开盘啦
#    格式: {股票6位代码: 细分概念名称(第一位)}
_PRIMARY_TAG_BOOST = {
    "002594": "新能源汽车",  # 比亚迪 - 主身份=新能源汽车整车
    "300750": "锂电池",      # 宁德时代 - 主身份=锂电池
    "600362": "金属铜",      # 江西铜业 - 主身份=金属铜
    "601127": "新能源汽车",  # 赛力斯
    "000625": "新能源汽车",  # 长安汽车
    "601633": "新能源汽车",  # 长城汽车
    "600104": "汽车整车",    # 上汽集团
    "000550": "汽车整车",    # 江铃汽车
    "601238": "汽车整车",    # 广汽集团
    "000800": "汽车整车",    # 一汽解放
    "600006": "汽车整车",    # 东风汽车
    "600213": "汽车整车",    # 亚星客车
    "600303": "汽车整车",    # 曙光股份
    "600375": "汽车整车",    # 汉马科技
    "600418": "新能源汽车",  # 江淮汽车
    "600686": "汽车整车",    # 金龙汽车
    "000957": "汽车整车",    # 中通客车
    "000980": "新能源汽车",  # 众泰汽车
    "002333": "汽车整车",    # 罗普斯金(转型)
    "002997": "新能源汽车",  # 瑞鹄模具
    "301029": "新能源汽车",  # 协和电子
    "688339": "氢能源",      # 亿华通
    # 有色-铜
    "000630": "金属铜",      # 铜陵有色
    "000878": "金属铜",      # 云南铜业
    "601899": "黄金概念",    # 紫金矿业(以金为主)
    "603993": "基本金属",    # 洛阳钼业
    # 白酒
    "000858": "白酒",        # 五粮液
    "600519": "白酒",        # 贵州茅台
    "000568": "白酒",        # 泸州老窖
    "600809": "白酒",        # 山西汾酒
    "000799": "白酒",        # 酒鬼酒
    "600779": "白酒",        # 水井坊
    "002304": "白酒",        # 洋河股份
    "000596": "白酒",        # 古井贡酒
    "603369": "白酒",        # 今世缘
    "600559": "白酒",        # 老白干酒
    "000860": "白酒",        # 顺鑫农业
    # 电力设备-锂电
    "002460": "锂矿",        # 赣锋锂业
    "002466": "锂矿",        # 天齐锂业
    "300014": "锂电池",      # 亿纬锂能
    "002074": "锂电池",      # 国轩高科
    "688005": "锂电池",      # 容百科技
    "300037": "锂电池",      # 新宙邦
    "002812": "光伏",        # 恩捷股份(隔膜为主,但列光伏/储能)
    "601012": "光伏",        # 隆基绿能
    "600438": "光伏",        # 通威股份
    "002459": "光伏",        # 晶澳科技
    "002129": "光伏",        # TCL中环
    "688223": "光伏",        # 晶科能源
    "300274": "储能",        # 阳光电源
    "600900": "水电",        # 长江电力
    "601985": "核电",        # 中国核电
}

# 2. 申万行业名 → 开盘啦风格名称修正 (去掉罗马数字、补充行业前缀)
_INDUSTRY_NAME_FIX: dict[str, str] = {
    "铜": "金属铜",
    "白酒Ⅱ": "白酒",
    "白酒Ⅲ": "白酒",
    "饮料Ⅱ": "饮料",
    "食品加工Ⅱ": "食品加工",
    "电动乘用车": "新能源汽车",  # 申万三级"电动乘用车"→更常见的"新能源汽车"概念
}

# 3. 概念名净化 (剔除"概念"二字, 与开盘啦短句风格对齐, 如"白酒概念"→"白酒")
def _strip_concept_suffix(name: str | None) -> str | None:
    if not name:
        return None
    name = name.strip()
    # 末尾带"概念"二字的去掉 (白酒概念→白酒, 但要保留"光伏概念"→"光伏" OK, "华为概念"→"华为"不OK? 谨慎白名单)
    _keep_concept = {"华为概念", "鸿蒙概念", "苹果概念", "华为汽车", "小米概念", "特斯拉"}
    if name not in _keep_concept and (name.endswith("概念") or name.endswith("板块")):
        name = name[:-2] if name.endswith("概念") else name[:-2]
    return name.strip() or None

# 4. 基于申万行业推导: 追加细分/大行业标签 (level 0=细分概念, 1=大行业概念)
#    返回 list[(name, level)]  (不包含已在PRIMARY里的)
def _derive_boost_tags(ind1: str|None, ind2: str|None, ind3: str|None, code: str) -> list[tuple[str,int]]:
    adds: list[tuple[str,int]] = []
    # 有色金属-工业金属-铜 → 追加大行业"有色金属"其实已是ind1, 细分名_INDUSTRY_NAME_FIX已处理
    # 但ind3可能非"铜"却属于铜产业链, 用行业名时已修正
    # 汽车-乘用车-电动乘用车 → 追加"汽车整车"(大行业)
    if ind3 == "电动乘用车":
        adds.append(("汽车整车", 1))  # 第2位大行业备选: 汽车整车比"汽车"更具体
    # 汽车-乘用车 (非电动, 申万旧分类) → "汽车整车"作为细分
    if ind3 == "乘用车" and ind1 == "汽车":
        adds.append(("汽车整车", 0))
    # 电力设备-电池-锂电池 → 追加"新能源汽车"作为大行业(锂电下游核心赛道)
    if ind3 == "锂电池":
        adds.append(("新能源汽车", 1))
    # 电力设备-电池-储能电池 等 → "储能"作为细分
    if ind3 and ("储能" in ind3):
        adds.append(("储能", 0))
    # 电力设备-光伏设备 → "光伏"作为细分
    if ind3 and "光伏" in ind3:
        adds.append(("光伏", 0))
    # 有色金属-贵金属 → "黄金概念"作为细分 (若ind2=贵金属)
    if ind2 == "贵金属" and ind1 == "有色金属":
        adds.append(("黄金概念", 0))
    return adds

# 5. 取首个非空且与已选不重复的字符串
def _pick_first(pool: list, exclude: set) -> str | None:
    for v in pool:
        if not v:
            continue
        if isinstance(v, str):
            v = v.strip()
        if not v:
            continue
        if v in exclude:
            continue
        return v
    return None


# 概念黑名单: 非主营相关的事件/杂项类, 所属板块展示时剔除
_CONCEPT_BLACKLIST = {
    "含H股", "B股", "AB股", "整体上市", "ST板块", "*ST板块", "ST",
    "央企国资改革", "地方国资改革", "债转股", "股权转让", "股权划转",
    "举牌", "并购重组", "参股银行", "参股券商", "参股保险", "参股基金",
    "分拆上市意愿", "分拆上市", "融资融券", "富时罗素概念股", "标普道琼斯A股",
    "沪股通", "深股通", "MSCI概念", "证金持股", "汇金持股", "养老金持股",
    "QFII重仓", "转融券标的", "融资融券标的", "股权激励", "员工持股",
    "增持回购", "回购", "送转填权", "高送转预期", "高送转",
    "新股与次新股", "注册制次新股", "核准制次新股", "科创板次新股",
    "新股", "次新股", "北交所概念", "创业板重组松绑", "壳资源",
    "低价股", "中字头股票", "上海国企改革", "广东国资改革", "深圳国资改革",
    "浙江国资改革", "江苏国资改革", "山东国资改革", "北京国资改革",
    "福建国资改革", "天津国资改革", "重庆国资改革", "安徽国资改革",
    "湖南国资改革", "湖北国资改革", "四川国资改革", "河南国资改革",
    "山西国资改革", "陕西国资改革", "辽宁国资改革", "江西国资改革",
    "广西国资改革", "云南国资改革", "贵州国资改革", "甘肃国资改革",
    "内蒙国资改革", "新疆国企改革", "西藏国资改革", "青海国资改革",
    "宁夏国资改革", "吉林国资改革", "黑龙江国资改革", "河北国资改革",
}

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


# ============================================================
# 行情源容灾 (20260906)
# 此前全站仅依赖新浪, 新浪接口偶发限流/不可用时选股、K线、个股分析全部瘫痪。
# 策略: 新浪为主源; 腾讯为备源(K线 ifzq.gtimg.cn / 快照 qt.gtimg.cn)。
# 熔断: 全市场级失败(如 fetch_spot_all)立即熔断 _SINA_COOLDOWN_SEC;
#       单只K线级失败按频次累计(_SINA_FAIL_THRESHOLD 次内熔断), 避免单次
#       超时就全局切源造成抖动 —— 修复: 初版单次失败即熔断, 而备源快照又
#       依赖本地股票池缓存(收盘后才生成), 导致盘中单次超时引发全站瘫痪。
# 板块映射(申万/概念树)仅新浪提供, 备源期间板块显示"—"属预期行为。
# ============================================================
_SINA_COOLDOWN_SEC = 600          # 主源熔断时长(秒)
_SINA_FAIL_WINDOW_SEC = 60        # 单只K线失败计数窗口
_SINA_FAIL_THRESHOLD = 5          # 窗口内失败次数达到阈值才熔断
_SOURCE_BREAKER = {"sina_down_until": 0.0, "fails": []}
_SOURCE_BREAKER_LOCK = threading.Lock()


def _sina_available() -> bool:
    with _SOURCE_BREAKER_LOCK:
        return time.time() >= _SOURCE_BREAKER.get("sina_down_until", 0.0)


def _record_sina_failure(reason: str = "", immediate: bool = False) -> None:
    """记录主源失败。immediate=全市场级失败直接熔断; 否则窗口内累计达阈值才熔断。"""
    with _SOURCE_BREAKER_LOCK:
        now = time.time()
        fails = [t for t in _SOURCE_BREAKER.get("fails", []) if now - t < _SINA_FAIL_WINDOW_SEC]
        fails.append(now)
        _SOURCE_BREAKER["fails"] = fails
        if not (immediate or len(fails) >= _SINA_FAIL_THRESHOLD):
            return
        _SOURCE_BREAKER["sina_down_until"] = now + _SINA_COOLDOWN_SEC
        _SOURCE_BREAKER["fails"] = []
    print(f"[source] [{bj_now()}] 新浪主源异常({reason[:80]}), 熔断{_SINA_COOLDOWN_SEC}s 内改用腾讯备源", flush=True)


_TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"


def _fetch_kline_tencent(symbol: str, datalen: int = 40) -> list[dict]:
    """腾讯日K备源: 返回与新浪一致的 [{day,open,high,low,close,volume}]。
    20260906 审计修复:
    - 复权模式由 qfq 改为不复权(param 不带 qfq), 与新浪主源口径一致,
      否则除权日附近两源价格体系不同, 切源时均线/止损位会跳变;
    - 腾讯K线成交量为手, 新浪为股, 必须×100 (实测茅台 45416手 vs 4541564股)。"""
    data = _get(_TENCENT_KLINE_URL, {"param": f"{symbol},day,,,{datalen}"}, timeout=10)
    node = {}
    if isinstance(data, dict):
        node = (data.get("data") or {}).get(symbol) or {}
    rows = node.get("day") or node.get("qfqday") or []
    out: list[dict] = []
    for r in rows:
        try:
            out.append({
                "day": r[0],
                "open": float(r[1]), "close": float(r[2]),
                "high": float(r[3]), "low": float(r[4]),
                "volume": float(r[5]) * 100,
            })
        except (IndexError, ValueError, TypeError):
            continue
    return out


# 东财备源 (20260906 审计新增): 腾讯也有WAF限频风险, 增加第三源提高容灾深度
_EM_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
_EM_SPOT_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"


def _em_secid(symbol: str) -> str:
    """新浪风格 symbol(sh600519) → 东财 secid(1.600519 沪 / 0.600519 深·北)"""
    code = symbol[-6:]
    return ("1." if symbol.startswith("sh") else "0.") + code


def _fetch_kline_eastmoney(symbol: str, datalen: int = 40) -> list[dict]:
    """东财日K第三备源(fqt=0 不复权, 与新浪一致)。
    klines 字段序: 日期,开,收,高,低,量(手),额(元) → 量×100 对齐新浪的股。"""
    data = _get(_EM_KLINE_URL, {
        "secid": _em_secid(symbol),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57",
        "klt": 101, "fqt": 0, "lmt": datalen, "end": "20500101",
    }, timeout=10)
    out: list[dict] = []
    if isinstance(data, dict):
        for line in ((data.get("data") or {}).get("klines") or []):
            p = line.split(",")
            try:
                out.append({
                    "day": p[0],
                    "open": float(p[1]), "close": float(p[2]),
                    "high": float(p[3]), "low": float(p[4]),
                    "volume": float(p[5]) * 100,
                })
            except (IndexError, ValueError, TypeError):
                continue
    return out


_TENCENT_SPOT_URL = "https://qt.gtimg.cn/q="


def _fetch_spot_tencent(codes: list[str]) -> list[dict]:
    """腾讯实时行情备源: qt.gtimg.cn 批量查询(GBK文本), 输出与新浪 spot 行对齐的字典。
    股票池由调用方提供(本地缓存), 本函数只负责刷新价格字段。"""
    import re as _re
    out: list[dict] = []
    CHUNK = 60
    for i in range(0, len(codes), CHUNK):
        chunk = codes[i:i + CHUNK]
        try:
            r = requests.get(_TENCENT_SPOT_URL + ",".join(chunk), headers=HEADERS, timeout=10)
            r.encoding = "gbk"
            text = r.text
        except Exception:  # noqa: BLE001
            continue
        for line in text.split(";"):
            line = line.strip()
            m = _re.search(r'v_(sh|sz|bj)(\d{6})="([^"]*)"', line)
            if not m:
                continue
            sym = m.group(1) + m.group(2)
            f = m.group(3).split("~")
            if len(f) < 46:
                continue
            try:
                def _f(idx, default=0.0):
                    try:
                        return float(f[idx])
                    except (ValueError, TypeError, IndexError):
                        return default
                out.append({
                    "code": m.group(2),
                    "symbol": sym,
                    "name": f[1],
                    "trade": _f(3),
                    "open": _f(5),
                    "high": _f(33),
                    "low": _f(34),
                    # 新浪 volume 单位=股, 腾讯=手; amount 新浪=元, 腾讯=万元; nmc 新浪=万元, 腾讯=亿元
                    "volume": _f(6) * 100,
                    "changepercent": _f(32),
                    "amount": _f(37) * 1e4,
                    "nmc": _f(44) * 1e4,
                })
            except Exception:  # noqa: BLE001
                continue
    return out


_UNIVERSE_FILE = os.path.join(CACHE_DIR, "universe_latest.json")


def _save_universe(rows: list[dict]) -> None:
    """保存股票池快照(代码/名称), 供主源故障时腾讯备源批量刷新价格 (20260906)"""
    try:
        slim = [{"code": r.get("code", ""), "symbol": r.get("symbol", ""), "name": r.get("name", "")}
                for r in rows if r.get("code")]
        if slim:
            _os.makedirs(CACHE_DIR, exist_ok=True)
            with open(_UNIVERSE_FILE, "w", encoding="utf-8") as f:
                _json.dump(slim, f, ensure_ascii=False)
    except OSError:
        pass


def _fetch_spot_eastmoney(codes: list[str]) -> list[dict]:
    """东财实时行情第三备源: ulist 批量查询, 输出与新浪 spot 行对齐。
    字段: f2现价 f3涨跌% f5量(手) f6额(元) f12代码 f14名称 f15高 f16低 f17开 f21流通市值(元)"""
    out: list[dict] = []
    CHUNK = 60
    for i in range(0, len(codes), CHUNK):
        chunk = codes[i:i + CHUNK]
        secids = ",".join(_em_secid(_to_symbol(c)) for c in chunk)
        try:
            data = _get(_EM_SPOT_URL, {
                "secids": secids,
                "fields": "f2,f3,f5,f6,f12,f14,f15,f16,f17,f21",
                "fltt": "2", "pn": 1, "pz": 200, "np": 1,
            }, timeout=10)
        except Exception:  # noqa: BLE001
            continue
        diff = ((data or {}).get("data") or {}).get("diff") or []
        for x in diff:
            try:
                code = str(x.get("f12", ""))
                if not code:
                    continue
                def _f(key, default=0.0):
                    v = x.get(key)
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        return default
                out.append({
                    "code": code,
                    "symbol": _to_symbol(code),
                    "name": x.get("f14", ""),
                    "trade": _f("f2"),
                    "open": _f("f17"),
                    "high": _f("f15"),
                    "low": _f("f16"),
                    "volume": _f("f5") * 100,          # 手→股
                    "changepercent": _f("f3"),
                    "amount": _f("f6"),                 # 元, 与新浪一致
                    "nmc": _f("f21") / 1e4,             # 元→万元(新浪口径)
                })
            except Exception:  # noqa: BLE001
                continue
    return out


def _load_any_spot_universe() -> list[dict]:
    """容灾用股票池: 优先读 universe_latest.json(每次成功拉取后更新, 内容为 list),
    其次读本地任一日期的快照缓存; 都没有则空。
    20260906 修复: 初版误用只接受 dict 的 _kv_read_local_file 读 list 型股票池,
    导致备源永远判定'无股票池缓存'。"""
    try:
        if os.path.isfile(_UNIVERSE_FILE):
            with open(_UNIVERSE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                return data
    except Exception:  # noqa: BLE001
        pass
    import glob as _glob
    files = sorted(_glob.glob(os.path.join(CACHE_DIR, "spot_*.json")), reverse=True)
    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                rows = json.load(f)
            if isinstance(rows, list) and rows:
                return [{"code": r.get("code", ""), "symbol": r.get("symbol", ""), "name": r.get("name", "")}
                        for r in rows if r.get("code")]
        except Exception:  # noqa: BLE001
            continue
    return []


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
    收盘后优先使用本地缓存; 盘中实时拉取。
    20260906 容灾: 新浪主源失败(或熔断期内)时, 用本地缓存的股票池 + 腾讯批量行情兜底。"""
    # 收盘后: 优先用本地缓存
    if _should_use_spot_cache():
        cache_date = _cache_date_for_fetch()
        cached = _load_spot_cache(cache_date)
        if cached is not None:
            return cached
    try:
        tried_sina = _sina_available()
        if not tried_sina:
            raise RuntimeError("新浪主源熔断中")
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
        if len(rows) < 100:
            raise RuntimeError(f"新浪快照仅返回{len(rows)}条, 判定主源异常")
        # 保存股票池供容灾使用 (20260906)
        _save_universe(rows)
        # 收盘后保存缓存
        if _should_use_spot_cache() and rows:
            _save_spot_cache(_cache_date_for_fetch(), rows)
        return rows
    except Exception as e:  # noqa: BLE001
        # ---- 腾讯备源 → 东财第三源: 本地股票池 + 批量刷新价格 ----
        # 20260906 修复: 仅在真正尝试过新浪后才记录失败; 熔断打开期间的失败
        # 不能再续期熔断, 否则熔断永不恢复(自续期死循环)
        if tried_sina:
            _record_sina_failure(str(e)[:80], immediate=True)
        universe = _load_any_spot_universe()
        if not universe:
            raise RuntimeError(f"新浪主源失败({str(e)[:60]})且本地无股票池缓存, 备源不可用") from e
        rows = _fetch_spot_tencent([u["symbol"] or u["code"] for u in universe])
        if not rows:
            rows = _fetch_spot_eastmoney([u["symbol"] or u["code"] for u in universe])
        if not rows:
            raise RuntimeError(f"新浪主源失败且腾讯/东财备源均无数据: {str(e)[:60]}") from e
        return rows


# ============================================================
# 数据层: 个股日K线 (含当日)
# ============================================================
def _fetch_kline_remote(symbol: str, datalen: int) -> list[dict]:
    """K线远程拉取三级源: 新浪 → 腾讯 → 东财 (20260906 容灾)"""
    out: list[dict] = []
    sina_err = ""
    tried_sina = _sina_available()
    if tried_sina:
        try:
            data = _get(SINA_KLINE, {"symbol": symbol, "scale": 240, "ma": "no", "datalen": datalen})
            if isinstance(data, list):
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
            if not out:
                sina_err = "新浪K线返回为空"
        except Exception as e:  # noqa: BLE001
            sina_err = str(e)[:80]
    if not out:
        # ---- 腾讯备源 → 东财第三源 ----
        if sina_err:
            _record_sina_failure(sina_err)  # 单只K线失败按频次累计, 不立即熔断 (20260906 修复)
        try:
            out = _fetch_kline_tencent(symbol, datalen)
        except Exception as e:  # noqa: BLE001
            print(f"[source] [{bj_now()}] 腾讯K线备源失败 {symbol}: {str(e)[:60]}, 改用东财", flush=True)
        if not out:
            try:
                out = _fetch_kline_eastmoney(symbol, datalen)
            except Exception as e:  # noqa: BLE001
                print(f"[source] [{bj_now()}] K线三源均失败 {symbol}: 新浪:{sina_err[:40]} / 东财:{str(e)[:60]}", flush=True)
                return []
    return out


def _kline_has_today(bars: list[dict] | None, today_str: str) -> bool:
    """判断K线序列最后一根是否已是最近交易日 (20260906 缓存策略核心判据)"""
    if not bars:
        return False
    return (bars[-1].get("day") or "")[:10].replace("-", "") >= today_str


def fetch_kline(symbol: str, datalen: int = 40) -> list[dict]:
    """返回 [{day,open,high,low,close,volume}, ...] 含最近交易日。
    20260906 缓存策略 (按用户要求):
      交易日15:00后刷新时, 先检查数据源是否有'今日最新数据':
      · 本地缓存已含最近交易日 → 直接用, 不再下载;
      · 缓存缺最近交易日 → 检查远端, 有今日数据则更新本地(之后不再下载),
        远端也没有则用spot快照补全当日bar并落盘;
      · 都没有今日数据 → 用历史数据顶上, 且【不落盘】(不锁死缓存),
        后续刷新会继续检查, 直到拿到今日最新数据。
    容灾: 新浪主源失败(或熔断期内)自动切换腾讯/东财备源。"""
    after_close = _is_after_close()
    cache_date = _cache_date_for_fetch()
    today_str = _latest_trade_date_str()

    cached = _load_kline_cache(cache_date, symbol)
    if cached is not None and len(cached) >= datalen:
        if not after_close:
            # 盘中/盘前/周末: 缓存含最近交易日即为有效(维持原有行为)
            return cached
        if _kline_has_today(cached, today_str):
            # 收盘后缓存已含今日最新数据 → 直接用, 不再下载
            return cached
        # 收盘后缓存缺今日 → 主动检查远端是否已更新出今日数据
        fresh = _fetch_kline_remote(symbol, datalen)
        if _kline_has_today(fresh, today_str):
            _save_kline_cache(cache_date, symbol, fresh)
            return fresh
        # 远端暂无今日 → 用spot快照补全当日bar, 成功则落盘
        patched = _patch_today_bar_from_spot(cached, symbol, today_str)
        if _kline_has_today(patched, today_str):
            _save_kline_cache(cache_date, symbol, patched)
            return patched
        # 都没有今日数据 → 用历史数据(不落盘, 下次刷新继续检查)
        return cached

    # 无缓存或缓存不足 → 远程拉取
    out = _fetch_kline_remote(symbol, datalen)
    if not out:
        return []
    if not _kline_has_today(out, today_str):
        # 源数据缺最近交易日 → 尝试用spot快照补全
        out = _patch_today_bar_from_spot(out, symbol, today_str)
    # 落盘策略:
    #   收盘后: 只有含最近交易日的完整数据才写入缓存(避免数据滞后时把不完整
    #   数据锁死一整天); 否则只用历史数据, 不落盘, 后续刷新继续检查。
    #   盘中/盘前: 维持原有落盘行为。
    if out and (_kline_has_today(out, today_str) or not after_close):
        _save_kline_cache(cache_date, symbol, out)
    return out


def _patch_today_bar_from_spot(bars: list[dict], symbol: str, today_str: str) -> list[dict]:
    """用spot实时快照数据补全今天的K线条目。
    symbol: 如 sh601398; today_str: YYYYMMDD格式。
    如果spot数据有今天的价格，追加一条K线; 否则原样返回。"""
    try:
        spot_all = fetch_spot_all()
    except Exception:
        return bars
    # 转成标准日期格式 YYYY-MM-DD
    today_iso = f"{today_str[:4]}-{today_str[4:6]}-{today_str[6:]}"
    for r in spot_all:
        if r.get("symbol") == symbol or r.get("code") == symbol.lstrip("shszbj"):
            try:
                trade = float(r.get("trade") or 0)
                if trade <= 0:
                    continue
                bar = {
                    "day": today_iso,
                    "open": float(r.get("open") or trade),
                    "high": float(r.get("high") or trade),
                    "low": float(r.get("low") or trade),
                    "close": trade,
                    "volume": float(r.get("volume") or 0),
                }
                # 如果最后一条已经是今天，替换；否则追加
                if bars and (bars[-1].get("day") or "")[:10] == today_iso:
                    bars[-1] = bar
                else:
                    bars.append(bar)
                return bars
            except (KeyError, ValueError, TypeError):
                return bars
    return bars


# ============================================================
# 数据层: 行业 + 概念板块映射
# ============================================================
def _build_board_maps():
    """构建 code -> 申万一级/二级行业, code -> [过滤后概念板块...] 映射 (缓存)"""
    now = time.time()
    if now - _board_cache["built_at"] < _BOARD_MAP_TTL and _board_cache["industry"]:
        return
    tree = _get(SINA_NODES)
    try:
        a_group = tree[1][0][1]  # "A股" 的子分类列表
    except (IndexError, TypeError):
        return
    sw1_items, sw2_items, sw3_items, concept_items = [], [], [], []
    for cat in a_group:
        if not (isinstance(cat, list) and len(cat) >= 2):
            continue
        catname, children = cat[0], cat[1]
        items = [c for c in children if isinstance(c, list) and len(c) >= 3]
        if catname == "申万一级":
            sw1_items = items
        elif catname == "申万二级":
            sw2_items = items
        elif catname == "申万三级":
            sw3_items = items
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
    industry2_map: dict[str, str] = {}
    industry3_map: dict[str, str] = {}
    concept_map: dict[str, list[str]] = {}

    # rank: 0=申万一级 1=申万二级 2=申万三级 3=概念板块
    lock = threading.Lock()
    all_items = ([(name, code, 0) for name, _, code in sw1_items] +
                 [(name, code, 1) for name, _, code in sw2_items] +
                 [(name, code, 2) for name, _, code in sw3_items] +
                 [(name, code, 3) for name, _, code in concept_items])

    def build_one(name, node_code, rank):
        try:
            members = fetch_node_members(node_code)
        except Exception:  # noqa: BLE001
            return
        with lock:
            if rank == 0:
                for c in members:
                    industry_map[c] = name
            elif rank == 1:
                for c in members:
                    industry2_map[c] = name
            elif rank == 2:
                for c in members:
                    industry3_map[c] = name
            else:
                if name in _CONCEPT_BLACKLIST:
                    return
                for c in members:
                    concept_map.setdefault(c, [])
                    if name not in concept_map[c]:
                        concept_map[c].append(name)

    futs = [POOL.submit(build_one, n, c, r) for (n, c, r) in all_items]
    for f in as_completed(futs):
        f.result()

    _board_cache["industry"] = industry_map
    _board_cache["industry2"] = industry2_map
    _board_cache["industry3"] = industry3_map
    _board_cache["concept"] = concept_map
    _board_cache["built_at"] = now


def get_industry(code: str) -> str:
    return _board_cache["industry"].get(code, "—")


def _clean_ind(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    # 去掉申万分类尾部的罗马数字后缀(ⅡⅢ等)
    while s and s[-1] in "ⅡⅢⅣⅤⅥⅦⅧⅨⅩ":
        s = s[:-1]
    return s.strip() or None


def get_concepts(code: str) -> str:
    """所属板块(前2个): 对齐"开盘啦"口径, 取 [细分概念, 大行业/大概念] 组合.

    优先级(从高到低, 合并去重后取前2位):
      A. 个股白名单第一标签 (头部权重股如宁德时代/比亚迪/江西铜业, 直接对齐开盘啦)
      B. 行业推导的细分标签 (level=0, 如电动乘用车→新能源汽车)
      C. 命名修正后的申万三级行业 (如 铜→金属铜, 白酒Ⅲ→白酒, 电动乘用车→新能源汽车)
      D. 申万二级行业
      --- 第2位(大行业/大概念)取: ---
      E. 行业推导的大行业标签 (level=1, 如锂电池→新能源汽车, 电动乘用车→汽车整车)
      F. 申万一级行业 (有色金属 / 电力设备 / 汽车 / 食品饮料 ...)
      G. 过滤净化后的新浪概念板块 (作为兜底)
    """
    ind1_raw = _board_cache["industry"].get(code)
    ind2_raw = _clean_ind(_board_cache["industry2"].get(code))
    ind3_raw = _clean_ind(_board_cache["industry3"].get(code))

    # 命名修正: 申万行业名 → 开盘啦风格
    ind1: str | None = _INDUSTRY_NAME_FIX.get(ind1_raw, ind1_raw)
    ind2: str | None = _INDUSTRY_NAME_FIX.get(ind2_raw, ind2_raw) if ind2_raw else None
    ind3: str | None = _INDUSTRY_NAME_FIX.get(ind3_raw, ind3_raw) if ind3_raw else None

    # 行业推导: 追加 boost 标签
    boost_tags = _derive_boost_tags(ind1_raw, ind2_raw, ind3_raw, code)
    boost_primary = [t for t, lv in boost_tags if lv == 0]   # 细分概念备选
    boost_big     = [t for t, lv in boost_tags if lv == 1]   # 大行业备选

    # 概念净化 (白酒概念→白酒, 过滤黑名单后净化)
    raw_concepts = _board_cache["concept"].get(code, []) or []
    clean_concepts: list[str] = []
    for c in raw_concepts:
        cc = _strip_concept_suffix(c)
        if cc:
            clean_concepts.append(cc)

    # ---- 取第1位: 细分概念/行业 ----
    # 优先级池 A→B→C→D
    primary_pool = ([_PRIMARY_TAG_BOOST.get(code)] +
                    boost_primary +
                    [ind3, ind2] +
                    clean_concepts)
    picked: list[str] = []
    first = _pick_first(primary_pool, set())
    if first:
        picked.append(first)

    # ---- 取第2位: 大行业/大概念 ----
    # 优先级池 E→F→(boost_primary兜底, 若未选)→B重复→D→G
    big_pool = (boost_big +
                [ind1] +
                boost_primary +     # 若细分标签落选也可做大行业 (如光伏/储能/黄金)
                [ind2, ind3] +
                clean_concepts)
    second = _pick_first(big_pool, set(picked))
    if second:
        picked.append(second)

    # 兜底: 若只有0或1个, 补新浪概念直到凑够2或耗尽
    if len(picked) < 2:
        tail_pool = [ind1, ind2, ind3] + clean_concepts
        more = _pick_first(tail_pool, set(picked))
        if more:
            picked.append(more)

    return "、".join(picked) if picked else "—"


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
# 交易日工具 (工作日近似, 不含节假日)
# ============================================================
def _trading_days_between(date1_str: str, date2_str: str) -> int:
    """date1, date2 形如 'YYYY-MM-DD'; 返回从 date1 到 date2 之间的交易日天数估计(用工作日近似, 不含节假日)。"""
    try:
        d1 = datetime.strptime(date1_str, "%Y-%m-%d").date()
        d2 = datetime.strptime(date2_str, "%Y-%m-%d").date()
    except Exception:
        return -1
    if d1 > d2:
        d1, d2 = d2, d1
    # 近似: 总天数 * 5/7 再校正
    total = (d2 - d1).days
    # 工作日计数:
    workdays = 0
    cur = d1
    step = 1 if total >= 0 else -1
    for _ in range(abs(total) + 1):
        if cur.weekday() < 5:
            workdays += 1
        cur += timedelta(days=step)
    return max(0, workdays - 1)  # 从 d1 到 d2 经过了多少天(不含d1当天)


def _add_trading_days(start_date_str: str, n: int):
    """从 start_date_str 起向后推 n 个交易日(工作日近似), 返回 'YYYY-MM-DD' 或 None。"""
    try:
        d = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    except Exception:
        return None
    added = 0
    cur = d
    while added < n:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            added += 1
    return cur.strftime("%Y-%m-%d")


# ============================================================
def board_limit(code: str) -> float:
    """涨跌停幅度(%)。主板10 / 创业板20 / 北交所30 / ST 5。未考虑ST细节时简化为板幅。"""
    if code.startswith(("300", "301")):
        return 20.0
    if code.startswith(("688",)):  # 科创板
        return 20.0
    if code.startswith(("8", "920", "43")):  # 北交所
        return 30.0
    return 10.0  # 主板 60/00


# A股异动监管基准 (板幅) 的偏离阈值
# 主板/ST : 3日偏离值 ±20% → 触发异常波动公告
# 创业板/科创板(20%): 3日偏离值 ±30% → 触发 (实际上是 ±30%)
def _abnormal_3d_threshold(code: str) -> float:
    """3 个交易日累计偏离值阈值(%); 超过则触发"股票交易异常波动"公告/监管"""
    lim = board_limit(code)
    if lim >= 20:
        return 30.0   # 20% 板幅股票: 30%
    return 20.0       # 10% 板幅股票: 20%


# 对应板块的对比指数(偏离值基准)
_INDEX_MAP = {
    "sh_main": "sh000001",   # 上证主板(60开头) → 上证指数
    "sz_main": "sz399001",   # 深证主板(00开头) → 深证成指
    "cyb":     "sz399006",   # 创业板(300开头) → 创业板指
    "kcb":     "sh000688",   # 科创板(688开头) → 科创50
}


def _index_symbol_for(code: str) -> str:
    """根据代码选择对应的偏离值对比指数"""
    if code.startswith("688"):
        return _INDEX_MAP["kcb"]
    if code.startswith(("300", "301")):
        return _INDEX_MAP["cyb"]
    if code.startswith("6"):
        return _INDEX_MAP["sh_main"]
    return _INDEX_MAP["sz_main"]


# 指数K线缓存 (按交易日)
_INDEX_KLINE_CACHE: dict[str, tuple[float, list[dict]]] = {}
_INDEX_KLINE_LOCK = threading.Lock()
_INDEX_KLINE_TTL = 8 * 3600  # 8小时


def _fetch_index_kline(code6: str, datalen: int = 25) -> list[dict]:
    """拉指数日K线(含当日): sh000001/sz399001 等; 按交易日缓存,8小时TTL。"""
    sym = code6
    cache_date = _cache_date_for_fetch()
    with _INDEX_KLINE_LOCK:
        cached = _INDEX_KLINE_CACHE.get(sym)
        if cached and (time.time() - cached[0]) < _INDEX_KLINE_TTL and len(cached[1]) >= datalen:
            # 日期一致直接返回
            return cached[1]
    bars = fetch_kline(sym, datalen=datalen)
    if bars:
        with _INDEX_KLINE_LOCK:
            _INDEX_KLINE_CACHE[sym] = (time.time(), bars)
    return bars


# ============================================================
# 股价异动 + 监管信息 (按用户上传规则)
# 规则要点 (来自用户上传 CSV):
#   1) 偏离值 = 个股涨幅 − 同期大盘指数涨幅 (非单纯个股涨幅)
#   2) 区间法: 期末收盘 / 期初收盘 − 1 (非每日偏离值累加)
#   3) 10个交易日偏离值 ≥ 100% → 触发严重异动 → 进重点监控 10 个交易日
#   4) 30个交易日偏离值 ≥ 200% → 触发严重异动 → 进重点监控 10 个交易日
#   5) 异动后从次一交易日开始重新计算
#   6) 创业板/主板/科创板规则有差异 (阈值相同, 对比指数不同)
# 前置门槛 (用户要求): 近30交易日中连续10天累计涨幅 > 70% 才计算, 否则不展示卡片
# 监管状态: 只由偏离值规则决定 (不用公告推断), 触发后进入10交易日监控期
# ============================================================
_INDEX_NAME_MAP = {"sh000001": "上证指数", "sz399001": "深证成指", "sz399006": "创业板指", "sh000688": "科创50"}


def calc_regulatory(code: str, bars: list[dict], limit_pct: float = None) -> dict:
    """股价异动 + 监管信息。前置门槛未过返回空字典, 调用方不展示卡片。"""
    if not bars or len(bars) < 31:
        return {}
    if limit_pct is None:
        limit_pct = board_limit(code)

    closes = [b["close"] for b in bars]
    days = [b.get("day", "")[:10] for b in bars]

    # ========== 前置门槛: 近30交易日中是否存在连续10天累计涨幅 > 70% ==========
    last30 = closes[-30:]
    pre_gate_passed = False
    pre_gate_max_10d = 0.0
    for i in range(len(last30) - 10 + 1):
        base = last30[i]
        if base <= 0:
            continue
        cum_10d = (last30[i + 10 - 1] / base - 1) * 100
        if cum_10d > pre_gate_max_10d:
            pre_gate_max_10d = cum_10d
        if cum_10d > 70:
            pre_gate_passed = True
    if not pre_gate_passed:
        return {}

    # ========== 对比指数 ==========
    idx_sym = _index_symbol_for(code)
    idx_bars = _fetch_index_kline(idx_sym, datalen=max(45, len(bars)))
    idx_closes = [b["close"] for b in idx_bars] if idx_bars else []

    N = min(len(closes), len(idx_closes)) if idx_closes else len(closes)
    closes_n = closes[-N:]
    idx_closes_n = idx_closes[-N:] if idx_closes else []
    days_n = days[-N:] if len(days) >= N else days

    # ========== 偏离值 (区间法 期末/期初) ==========
    def _ret(arr, window):
        if len(arr) < window + 1 or arr[-(window + 1)] == 0:
            return None
        return (arr[-1] / arr[-(window + 1)] - 1) * 100

    ret_10d = _ret(closes_n, 10)
    ret_30d = _ret(closes_n, 30)
    idx_10d = _ret(idx_closes_n, 10) if idx_closes_n else 0
    idx_30d = _ret(idx_closes_n, 30) if idx_closes_n else 0

    dev_10d = (ret_10d - idx_10d) if ret_10d is not None else None
    dev_30d = (ret_30d - idx_30d) if ret_30d is not None else None

    # ========== 触发条件 ==========
    TRIG_10D = 100.0
    TRIG_30D = 200.0
    MONITOR_TOTAL = 10

    triggered_10d = (dev_10d is not None and dev_10d >= TRIG_10D)
    triggered_30d = (dev_30d is not None and dev_30d >= TRIG_30D)
    triggered = triggered_10d or triggered_30d

    to_trig_10d = round(max(0.0, TRIG_10D - (dev_10d if dev_10d is not None else -999)), 2)
    to_trig_30d = round(max(0.0, TRIG_30D - (dev_30d if dev_30d is not None else -999)), 2)
    warn_10d = (not triggered_10d) and (dev_10d is not None) and (to_trig_10d <= 20.0)
    warn_30d = (not triggered_30d) and (dev_30d is not None) and (to_trig_30d <= 20.0)
    nearing = triggered or warn_10d or warn_30d

    # ========== 监控期: 只由偏离值规则决定, 扫描找到首次触发日 ==========
    # 向前扫描: 找到最近一次偏离值首次跨越阈值的交易日, 即为监控开始日
    today_bj = datetime.now(_BJ_TZ).strftime("%Y-%m-%d")
    monitor_activated = False
    monitor_start = None
    monitor_end = None
    monitor_day = 0
    monitor_days_left = 0
    monitor_trigger_type = ""  # 10日偏离值 / 30日偏离值

    def _dev_at(k, window):
        """计算第 k 天的 window 日偏离值(个股涨幅 - 指数涨幅)"""
        if k < window or k >= len(closes_n):
            return None
        base_s = closes_n[k - window]
        base_i = idx_closes_n[k - window] if (idx_closes_n and k < len(idx_closes_n)) else 0
        if base_s == 0 or base_i == 0:
            return None
        rs = (closes_n[k] / base_s - 1) * 100
        ri = (idx_closes_n[k] / base_i - 1) * 100 if (idx_closes_n and k < len(idx_closes_n)) else 0
        return rs - ri

    if triggered:
        # 优先用 30日偏离值触发(更严重), 否则 10日
        win = 30 if triggered_30d else 10
        thr_t = TRIG_30D if triggered_30d else TRIG_10D
        monitor_trigger_type = f"{win}日偏离值"
        # 从当前往前找: 找到偏离值 < 阈值 的最近一天, 那天的次一交易日 = 触发日(监控开始日)
        trigger_idx = len(closes_n) - 1  # 默认=今天(找不到更早的)
        for k in range(len(closes_n) - 1, win - 1, -1):
            dv = _dev_at(k, win)
            if dv is not None and dv < thr_t:
                trigger_idx = k + 1  # 次一交易日开始触发
                break
        # trigger_idx 对应的日期 = 监控开始日
        if trigger_idx < len(days_n):
            monitor_start = days_n[trigger_idx]
        elif days_n:
            monitor_start = days_n[-1]
        # 监控结束日 = 开始日 + 10 交易日
        if monitor_start:
            monitor_end = _add_trading_days(monitor_start, MONITOR_TOTAL)
            td = _trading_days_between(monitor_start, today_bj)
            day = (td + 1) if td >= 0 else 1
            if day <= MONITOR_TOTAL:
                monitor_activated = True
                monitor_day = day
                monitor_days_left = MONITOR_TOTAL - day
            else:
                # 监控期已过
                monitor_activated = False
                monitor_day = 0
                monitor_days_left = 0

    # ========== 异动规则文本 (前端可折叠) ==========
    board_label = "主板10%" if limit_pct == 10 else ("双创板20%" if limit_pct == 20 else f"板幅{limit_pct}%")
    rules_text = (
        "【异动判定规则】\n"
        "1. 偏离值 = 个股涨幅 − 同期大盘指数涨幅（非单纯个股涨幅）\n"
        "2. 区间法：期末收盘 / 期初收盘 − 1（非每日偏离值累加）\n"
        f"3. 10个交易日偏离值 ≥ 100% → 触发严重异动 → 进重点监控 {MONITOR_TOTAL} 个交易日\n"
        f"4. 30个交易日偏离值 ≥ 200% → 触发严重异动 → 进重点监控 {MONITOR_TOTAL} 个交易日\n"
        "5. 异动后从次一交易日开始重新计算\n"
        f"6. 对比指数：{idx_sym}（{_INDEX_NAME_MAP.get(idx_sym, idx_sym)}）\n"
        f"7. 板幅：{board_label}\n"
        "8. 前置门槛：近30交易日中连续10天累计涨幅＞70%才计算异动"
    )

    # ========== 摘要 ==========
    parts = []
    if triggered:
        if monitor_activated:
            parts.append(f"🛑 已触发严重异动 · 重点监控期第 {monitor_day}/{MONITOR_TOTAL} 天（{monitor_trigger_type}达标）")
        else:
            parts.append(f"🔥 已触发严重异动（{monitor_trigger_type}达标）· 监控期已过")
    else:
        bits = []
        if warn_10d:
            bits.append(f"10日偏离值 {dev_10d:+.1f}% · 距100%还差 {to_trig_10d:.1f} 点")
        if warn_30d:
            bits.append(f"30日偏离值 {dev_30d:+.1f}% · 距200%还差 {to_trig_30d:.1f} 点")
        if bits:
            parts.append("⏳ " + " / ".join(bits))
        else:
            parts.append(f"前置门槛已过 · 暂未触发异动（10日偏离值 {dev_10d:+.1f}% / 30日偏离值 {dev_30d:+.1f}%）")
    summary = " | ".join(parts[:4])

    return {
        "limit_pct": limit_pct,
        "index_symbol": idx_sym,
        "index_name": _INDEX_NAME_MAP.get(idx_sym, idx_sym),
        "pre_gate_passed": True,
        "pre_gate_max_10d_pct": round(pre_gate_max_10d, 2),
        "ret_10d_pct": round(ret_10d, 2) if ret_10d is not None else None,
        "ret_30d_pct": round(ret_30d, 2) if ret_30d is not None else None,
        "idx_10d_pct": round(idx_10d, 2) if idx_10d is not None else None,
        "idx_30d_pct": round(idx_30d, 2) if idx_30d is not None else None,
        "dev_10d_pct": round(dev_10d, 2) if dev_10d is not None else None,
        "dev_30d_pct": round(dev_30d, 2) if dev_30d is not None else None,
        "triggered": bool(triggered),
        "triggered_10d": bool(triggered_10d),
        "triggered_30d": bool(triggered_30d),
        "to_trig_10d_pct": to_trig_10d,
        "to_trig_30d_pct": to_trig_30d,
        "warn_10d": bool(warn_10d),
        "warn_30d": bool(warn_30d),
        "nearing": bool(nearing),
        "monitor_activated": bool(monitor_activated),
        "monitor_trigger_type": monitor_trigger_type,
        "monitor_start": monitor_start,
        "monitor_end": monitor_end,
        "monitor_day": monitor_day,
        "monitor_days_total": MONITOR_TOTAL,
        "monitor_days_left": monitor_days_left,
        "rules_text": rules_text,
        "summary": summary,
    }


def daily_changes(bars: list[dict]) -> list[float]:
    """每根K线相对前一日收盘的涨幅%(首根为 nan)"""
    out = [float("nan")] * len(bars)
    for i in range(1, len(bars)):
        prev = bars[i - 1]["close"]
        if prev:
            out[i] = (bars[i]["close"] / prev - 1) * 100
    return out


def calc_macd(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD(12,26,9). 返回 (dif, dea, macd_hist) 序列, 与输入等长"""
    def ema(vals, n):
        out = [float("nan")] * len(vals)
        if len(vals) < n:
            return out
        k = 2 / (n + 1)
        # 找到第一个非nan的位置
        start = 0
        while start < len(vals) and math.isnan(vals[start]):
            start += 1
        if start + n > len(vals):
            return out
        out[start + n - 1] = sum(vals[start:start + n]) / n
        for i in range(start + n, len(vals)):
            if math.isnan(vals[i]):
                out[i] = out[i - 1]
            else:
                out[i] = vals[i] * k + out[i - 1] * (1 - k)
        return out
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    dif = [f - s if not (math.isnan(f) or math.isnan(s)) else float("nan")
           for f, s in zip(ema_fast, ema_slow)]
    dea = ema(dif, signal)
    hist = [(d - e) if not (math.isnan(d) or math.isnan(e)) else float("nan")
            for d, e in zip(dif, dea)]
    return dif, dea, hist


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


# ============================================================
# 量价关系量化体系 — 【双模式优化版】 波段票 × 趋势票
# 用户要求: 同一"缩量回调"对趋势票是低吸、对波段票是该走——赚的不是同一段钱
# 文档第二~七节: 参数对照表(§二) / 五种状态两种读法(§三) / 波段IF-THEN(§四) / 趋势IF-THEN(§五) / 风控差异(§六) / §七一句话纪律
# ============================================================
def _classify_style_light(closes: list, ma5: list, ma10: list, ma20: list,
                          highs: list, lows: list) -> tuple:
    """轻量级风格判定: 返回 (style_label, style_code, trend_score, swing_score)
    style_code ∈ {'trend', 'band', 'mix', 'avoid'}
    """
    import statistics
    N = len(closes)
    if N < 30:
        return ("混合型 · 两种模式都参考", "mix", 50, 50)
    c = closes[-1]
    ok_ma = (len(ma5)>0 and len(ma10)>0 and len(ma20)>0
             and not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) and not math.isnan(ma20[-1]) and ma20[-1] > 0)
    trend_score = 50.0
    swing_score = 50.0
    # 1. 均线排列
    if ok_ma and ma5[-1] > ma10[-1] > ma20[-1] and c > ma20[-1]:
        trend_score += 30; swing_score -= 10
    elif ok_ma and ma5[-1] < ma10[-1] < ma20[-1] and c < ma20[-1]:
        trend_score -= 15; swing_score += 5
    elif ok_ma and not (ma5[-1] > ma10[-1] > ma20[-1]) and not (ma5[-1] < ma10[-1] < ma20[-1]):
        trend_score -= 15; swing_score += 25
    # 2. MA20斜率(近4日)
    if ok_ma and N >= 4 and ma20[-4] > 0:
        slope = (ma20[-1] / ma20[-4] - 1) * 100
        if   slope >= 2:  trend_score += 20
        elif slope >= 1:  trend_score += 10
        elif slope >= 0:  trend_score += 3
        elif slope > -1:  swing_score += 5
        else:             swing_score += 12
    # 3. 近60日净涨跌幅
    if N >= 60 and closes[-60] > 0:
        chg60 = (closes[-1] / closes[-60] - 1) * 100
        if   chg60 >= 15: trend_score += 15
        elif chg60 >= 5:  trend_score += 8
        elif chg60 >= 0:  trend_score += 2
        elif chg60 > -8:  swing_score += 8
        else:             swing_score += 4; trend_score -= 5
    # 4. 近20日正收益日占比
    last20 = closes[-20:]
    ups = sum(1 for i in range(1,len(last20)) if last20[i] >= last20[i-1])
    pos_ratio = ups / max(1, len(last20)-1)
    if   pos_ratio >= 0.65: trend_score += 10
    elif pos_ratio <= 0.40: swing_score += 8
    # 5. ATR波动
    if N >= 15:
        atrs = []
        for i in range(N-min(14,N-1), N):
            atrs.append(abs(highs[i]-lows[i])/max(closes[i],1e-6)*100)
        atr_pct = sum(atrs)/len(atrs)
        if   atr_pct >= 5: swing_score += 10
        elif atr_pct <= 2: trend_score += 5
    diff = trend_score - swing_score
    if diff >= 12:
        return ("趋势票 · 不破位不走", "trend", trend_score, swing_score)
    elif diff <= -12:
        return ("波段票 · 不放量不拿", "band", trend_score, swing_score)
    else:
        return ("混合型 · 双模式都可参考", "mix", trend_score, swing_score)


_VP_MODE_PARAMS = {
    # ========== §二 参数对照表 ==========
    "band": {
        "name": "波段票",
        "zh": "波段票（3~15天快进快出）",
        "hold_cycle": "3~15 个交易日",
        "ma_anchor_label": "MA10",
        "ma_stop_label": "MA10",
        "vr_high": 2.0,
        "vr_low":  0.7,
        "pullback_tol": 5.0,
        "stop_below_ma_pct": 1.5,
        "huge_vr": 2.5,
        "high_buy_fobid_vr": 1.8,
        "position_weakened": True,
        "position_single_size": "20%~30%",
        "attitude_shrink": "警惕（默认热度退潮，换股）",
    },
    "trend": {
        "name": "趋势票",
        "zh": "趋势票（1~6个月，拿住主升浪）",
        "hold_cycle": "1~6 个月",
        "ma_anchor_label": "MA20",
        "ma_stop_label": "MA20",
        "vr_high": 1.5,
        "vr_low":  0.8,
        "pullback_tol": 8.0,
        "stop_below_ma_pct": 2.0,
        "huge_vr": 3.0,
        "high_buy_fobid_vr": 2.0,
        "position_weakened": False,
        "position_single_size": "30%~50%",
        "attitude_shrink": "友好（缩量回调是低吸机会）",
    },
}


def _trend_strength_level(c: float, ma5: list, ma10: list, ma20: list) -> dict:
    """
    趋势强度分级 → 决定止损锚定均线。
    返回 {level, label, anchor_ma, anchor_label, break_pct, desc}
      level: 'super' / 'strong' / 'normal' / 'weak'
      anchor_ma: 实际锚定的均线值(MA5/MA10/MA20)
      break_pct: 跌破锚定均线百分之几触发止损
    判定规则 (用户确认):
      超强势: MA5>MA10>MA20 多头排列 + MA5斜率陡 + 乖离率适中 → 锚 MA5, 跌破2%止盈离场
      强势:   多头排列 + MA10斜率向上 → 锚 MA10, 跌破1.5%离场
      普通:   多头但均线缠绕 → 锚 MA20(趋势票)/MA10(波段票), 按原规则 (调用方决定)
      弱势:   空头/跌破MA20 → 不操作或反弹离场
    """
    ok = (len(ma5) > 0 and len(ma10) > 0 and len(ma20) > 0
          and not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) and not math.isnan(ma20[-1])
          and ma20[-1] > 0)
    if not ok:
        return {"level": "normal", "label": "普通", "anchor_ma": None, "anchor_label": "MA20",
                "break_pct": 2.0, "desc": "均线数据不足, 默认普通"}
    m5, m10, m20 = ma5[-1], ma10[-1], ma20[-1]
    # 多头排列: MA5>MA10>MA20 且 收盘价>MA20
    is_bull_align = (m5 > m10 > m20 and c > m20)
    # 空头排列: MA5<MA10<MA20 且 收盘价<MA20
    is_bear_align = (m5 < m10 < m20 and c < m20)
    # MA5斜率(近3日): 陡峭向上 = 主升浪特征
    ma5_slope = 0.0
    if len(ma5) >= 4 and not math.isnan(ma5[-4]) and ma5[-4] > 0:
        ma5_slope = (m5 / ma5[-4] - 1) * 100
    # MA10斜率(近4日)
    ma10_slope = 0.0
    if len(ma10) >= 5 and not math.isnan(ma10[-5]) and ma10[-5] > 0:
        ma10_slope = (m10 / ma10[-5] - 1) * 100
    # 乖离率 (收盘价相对MA5)
    bias_ma5 = ((c / m5) - 1) * 100 if m5 > 0 else 0
    # ----- 分级 -----
    if is_bear_align:
        return {"level": "weak", "label": "弱势/空头", "anchor_ma": m20, "anchor_label": "MA20",
                "break_pct": 2.0,
                "desc": f"空头排列(MA5{m5:.2f}<MA10{m10:.2f}<MA20{m20:.2f}), 不操作或反弹离场"}
    if is_bull_align:
        # 超强势: 多头排列 + MA5斜率≥1.5%(陡峭) + 乖离率≤8%(适中, 没严重超买)
        if ma5_slope >= 1.5 and bias_ma5 <= 8.0:
            return {"level": "super", "label": "超强势", "anchor_ma": m5, "anchor_label": "MA5",
                    "break_pct": 2.0,
                    "desc": f"超强势: 多头排列 + MA5斜率{ma5_slope:+.2f}%陡峭 + 乖离MA5 {bias_ma5:+.1f}%适中 → 锚MA5, 跌破2%锁大肉离场"}
        # 强势: 多头排列 + MA10斜率向上(≥0.3%)
        if ma10_slope >= 0.3:
            return {"level": "strong", "label": "强势", "anchor_ma": m10, "anchor_label": "MA10",
                    "break_pct": 1.5,
                    "desc": f"强势: 多头排列 + MA10斜率{ma10_slope:+.2f}%向上 → 锚MA10, 跌破1.5%离场"}
        # 普通多头 (多头排列但均线走平/MA10斜率弱)
        return {"level": "normal", "label": "普通多头", "anchor_ma": m20, "anchor_label": "MA20",
                "break_pct": 2.0,
                "desc": f"普通: 多头排列但MA10斜率{ma10_slope:+.2f}%偏弱 → 锚MA20(趋势)/MA10(波段), 按原规则"}
    # 非多非空 (缠绕/震荡)
    return {"level": "normal", "label": "震荡缠绕", "anchor_ma": m20, "anchor_label": "MA20",
            "break_pct": 2.0,
            "desc": f"均线缠绕(MA5{m5:.2f}/MA10{m10:.2f}/MA20{m20:.2f}无明确排列) → 锚MA20(趋势)/MA10(波段), 按原规则"}


def _vp_single(bars: list, closes: list, highs: list,
               lows: list, vols: list, chgs: list,
               ma5: list, ma10: list, ma20: list,
               mode: str) -> dict:
    """单模式量价6步分析。mode ∈ {'band','trend'}。"""
    import statistics
    P = _VP_MODE_PARAMS[mode]
    N = len(closes)
    if N < 30: return {}

    last_close = closes[-1]
    today_open = bars[-1]["open"]; today_high = highs[-1]; today_low = lows[-1]
    pct = chgs[-1] if chgs and not math.isnan(chgs[-1]) else 0.0
    is_bull = last_close >= today_open
    is_bear = last_close <  today_open
    ok_ma = (len(ma5)>0 and len(ma10)>0 and len(ma20)>0
             and not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) and not math.isnan(ma20[-1]) and ma20[-1] > 0)
    anchor_ma = None; stop_ma = None
    if mode == "band":
        anchor_ma = ma10[-1] if (ok_ma and not math.isnan(ma10[-1])) else None
        stop_ma   = ma10[-1] if (ok_ma and not math.isnan(ma10[-1])) else None
    else:
        anchor_ma = ma20[-1] if (ok_ma and not math.isnan(ma20[-1])) else None
        stop_ma   = ma20[-1] if (ok_ma and not math.isnan(ma20[-1])) else None

    avg5_vol = sum(vols[-6:-1]) / 5 if N >= 6 else (sum(vols[:-1]) / max(1,len(vols)-1))
    if avg5_vol <= 0: vr = 1.0
    else:            vr = vols[-1] / avg5_vol
    vr_prev = 1.0
    if N >= 7:
        prev5_avg = sum(vols[-7:-2]) / 5
        vr_prev = vols[-2] / prev5_avg if prev5_avg > 0 else 1.0
    one_day_flash = (vr_prev >= 1.5) and (vr < 0.8)

    lb = min(250, N); low_250 = min(lows[-lb:])
    gain_from_bottom = (last_close / low_250 - 1) * 100 if low_250 > 0 else 0
    look_1y = min(250, N); h1y = max(highs[-look_1y:]); l1y = min(lows[-look_1y:])
    pos_1y = ((last_close - l1y) / (h1y - l1y) * 100) if h1y > l1y else 50

    recent_20_chgs = [x for x in chgs[-20:] if x is not None and not math.isnan(x)]
    sigma = statistics.pstdev(recent_20_chgs) if len(recent_20_chgs) >= 8 else None
    if sigma is None or sigma <= 0.4: sigma = 1.5
    up_sigma_factor = 0.9 if mode == "band" else 0.8
    up_thresh   =  up_sigma_factor * sigma
    down_thresh = -up_sigma_factor * sigma

    body_pct = abs(last_close - today_open) / today_open * 100 if today_open > 0 else 0
    upper_shadow = today_high - max(today_open, last_close)
    lower_shadow = min(today_open, last_close) - today_low
    body_abs = abs(last_close - today_open) if abs(last_close - today_open) > 1e-8 else 0.01
    small_body = body_pct <= 2
    long_lower_shadow = lower_shadow >= 2 * body_abs
    stab_sun = (pct >= 0) and (len(closes) >= 3 and closes[-1] > closes[-2])
    engulf = False
    if N >= 2:
        po = bars[-2]["open"]; pc = bars[-2]["close"]
        if pc < po:
            engulf = (bars[-1]["open"] <= pc) and (bars[-1]["close"] >= po) and (bars[-1]["close"] > bars[-1]["open"])
    band_reverse = False
    if N >= 2 and mode == "band" and closes[-2] < bars[-2]["open"]:
        if closes[-1] > bars[-1]["open"] and (closes[-1] - bars[-1]["open"]) >= (bars[-2]["open"] - closes[-2]):
            if vr >= P["vr_high"]:
                band_reverse = True
    stab_signal = stab_sun or engulf or long_lower_shadow

    # Step1 定性
    if   vr >= P["vr_high"]:  vol_tag = "放量";  vol_qual = "放"
    elif vr <= P["vr_low"]:   vol_tag = "缩量";  vol_qual = "缩"
    elif 0.8 < vr < 1.2:      vol_tag = "量平";  vol_qual = "平"
    else:                     vol_tag = "温和量";vol_qual = "平"
    if   pct >= up_thresh and is_bull:     price_tag = "价涨"; price_qual = "涨"
    elif pct <= down_thresh and is_bear:   price_tag = "价跌"; price_qual = "跌"
    else:                                   price_tag = "价平"; price_qual = "平"
    if mode == "band":
        if ok_ma and ma5[-1] > ma10[-1] and last_close > (ma10[-1] if ma10[-1] else 0):
            trend_tag = "上升趋势(MA10上)"
        elif ok_ma and ma5[-1] < ma10[-1] and last_close < (ma10[-1] if ma10[-1] else 9e9):
            trend_tag = "下跌趋势(MA10下)"
        else:
            trend_tag = "震荡/区间(MA10附近)"
    else:
        if ok_ma and ma10[-1] > ma20[-1] and last_close > ma20[-1]:
            trend_tag = "上升趋势(MA20上)"
        elif ok_ma and ma10[-1] < ma20[-1] and last_close < ma20[-1]:
            trend_tag = "下跌趋势(MA20下)"
        else:
            trend_tag = "震荡/中继(MA20附近)"
    if P["position_weakened"]:
        h20 = max(highs[-20:]); l20 = min(lows[-20:])
        pos_20_local = (last_close - l20) / max(h20-l20,1e-6) * 100
        if   pos_20_local <= 30: pos_tag = "靠近区间下沿（支撑位）"; pos_qual = "底"
        elif pos_20_local <= 70: pos_tag = "区间中部"; pos_qual = "中"
        else:                    pos_tag = "靠近区间上沿（压力位）"; pos_qual = "高"
    else:
        if   gain_from_bottom < 30:   pos_tag = "底部";   pos_qual = "底"
        elif gain_from_bottom <= 100: pos_tag = "中继区"; pos_qual = "中"
        else:                         pos_tag = "高位";   pos_qual = "高"
        if (gain_from_bottom > 100) or (pos_1y > 80):
            pos_tag = "高位"; pos_qual = "高"
    high_60 = max(highs[-60:]) if N >= 5 else highs[-1]
    drawdown_from_high = (high_60 - last_close) / high_60 * 100 if high_60 > 0 else 0
    pullback_ok = drawdown_from_high <= P["pullback_tol"]
    # 支撑位 = 锚定均线本身（波段=MA10, 趋势=MA20），不再取 max(MA, 20日收盘高*0.97) 以免数值与标签不符
    support = anchor_ma if (anchor_ma and anchor_ma > 0) else (low_250 * 1.03)
    # 止损线 = 支撑位 × (1 - stop_below_ma_pct%)，给出具体价格
    stop_loss_price = support * (1 - P["stop_below_ma_pct"] / 100) if support > 0 else None
    low_pierce = today_low < stop_loss_price if stop_loss_price else (today_low < support * 0.98)
    close_back = last_close >= stop_loss_price if stop_loss_price else (last_close >= support * 0.98)
    no_break_support = (not low_pierce) or close_back
    shrink_days = 0
    for i in range(N-2, max(0, N-8), -1):
        base = sum(vols[max(0,i-6):i]) / 5 if i >= 5 else 1
        vi = vols[i] / base if base > 0 else 1
        if vi <= P["vr_low"]: shrink_days += 1
        else: break

    tags = {
        "mode": f"{P['zh']} · 持仓{P['hold_cycle']}",
        "vol": f"{vol_tag}（VR {vr:.2f}，阈值放≥{P['vr_high']}/缩≤{P['vr_low']}）· 态度：{P['attitude_shrink']}" if vol_qual=="缩" else f"{vol_tag}（VR {vr:.2f}，阈值放≥{P['vr_high']}/缩≤{P['vr_low']}）",
        "price": f"{price_tag}（{pct:+.2f}%，阈值涨≥{up_thresh:+.1f}%/跌≤{down_thresh:+.1f}% · σ近20日={sigma:.2f}%，{'波段略敏感(0.9σ)' if mode=='band' else '趋势标准(0.8σ)'}）",
        "trend": f"{trend_tag} · 锚定均线 {P['ma_anchor_label']}={anchor_ma:.2f}" if anchor_ma else trend_tag,
        "position": pos_tag if P["position_weakened"] else f"{pos_tag}（距底部 {gain_from_bottom:+.1f}%，近一年分位 {pos_1y:.0f}%）",
        "pullback": f"{'回撤合规' if pullback_ok else '⚠️回撤超限'}（距高点回撤 {drawdown_from_high:.1f}%，阈值≤{P['pullback_tol']}% · {'波段弹性窗口窄' if mode=='band' else '趋势容忍洗盘'}）",
        "support": f"{'不破' if no_break_support else '⚠️破'}{P['ma_stop_label']}支撑位 {support:.2f}（今日最低 {today_low:.2f}，止损线 {stop_loss_price:.2f} = {P['ma_stop_label']}×{100-P['stop_below_ma_pct']:.1f}%）",
        "kline": ("小阴小阳" if small_body else "实体内含波") + f"（实体 {body_pct:.1f}%，阈值≤2%）",
        "stabilize": "✅有企稳信号（" + "、".join(filter(None,[
            "止跌阳线" if stab_sun else "",
            "阳包阴" if engulf else "",
            "放量反包阳(波段B2)" if band_reverse else "",
            "下影线≥实体2倍" if long_lower_shadow else "",
        ])) + "）" if stab_signal else "无企稳信号",
        "flash": f"⚠️量能一日游（前VR={vr_prev:.1f}→今VR={vr:.2f}）→ {'波段当日离场' if mode=='band' else '趋势转为观望'}" if one_day_flash else f"量能持续性正常（前日VR {vr_prev:.2f}）",
        "shrink_callback_days": f"已连续缩量回调 {shrink_days} 天（{'波段只等≤3天反包' if mode=='band' else '趋势容忍，看企稳'}）",
    }

    # Step2 量价状态
    if vol_qual == "放":
        if   price_qual == "涨": s_code="S1"; s_name="①量增价涨"
        elif price_qual == "平": s_code="S2"; s_name="⚠️放量滞涨"
        else:                    s_code="S3"; s_name="⚠️放量下跌"
    elif vol_qual == "平":
        if   price_qual == "涨": s_code="S4"; s_name="温和上涨"
        elif price_qual == "平": s_code="S5"; s_name="④量平价平"
        else:                    s_code="S6"; s_name="温和下跌"
    else:
        if   price_qual == "涨": s_code="S7"; s_name="③量缩价涨"
        elif price_qual == "平": s_code="S8"; s_name="缩量整理"
        else:
            if "上升" in trend_tag: s_code="S9";  s_name="②缩量回调"
            else:                   s_code="S10"; s_name="⑤量缩价跌"
    if mode == "band":
        read_map = {
            "S1": f"波段解读：VR必须 ≥ {P['vr_high']:.1f}才算有效启动（当前VR {vr:.2f} {'✅达标' if vr>=P['vr_high'] else '❌未达'}）；次日VR掉回0.8以下 = 脉冲失败，当天离场不观望",
            "S2": "波段解读：放量滞涨 = 短线资金不接力，立即减仓不犹豫",
            "S3": "波段解读：放量下跌 = 资金踩踏，直接离场，不抄底",
            "S4": "波段解读：温和上涨 = 力度不足，观察为主，不追",
            "S5": "波段解读【主战场】：区间操作 → 下沿企稳低吸，上沿无量兑现；放量突破上沿 → 转突破跟单",
            "S6": "波段解读：温和下跌 → 除非立刻反包，否则不接",
            "S7": "波段解读【警惕】：高位量缩价涨 = 动能衰减，不加仓，止盈位上移到MA10；一旦滞涨立即兑现",
            "S8": "波段解读：缩量整理 = 观察等待区间下沿机会",
            "S9": f"波段解读【默认不参与】：缩量 = 热度退潮，资金效率为王。唯一例外：缩量回调 ≤3天（当前{shrink_days}天{'✅可等反包' if shrink_days<=3 else '❌超时'}）+ 今日放量反包阳（当前{'✅有' if band_reverse else '❌无'}）→ 快速跟一笔；否则换股",
            "S10": "波段解读【直接跳过】：波段永远不抄底，只做右侧启动",
        }
    else:
        tol = P["pullback_tol"]
        read_map = {
            "S1": f"趋势解读：VR ≥ {P['vr_high']:.1f} + 突破平台即可跟进（当前VR {vr:.2f} {'✅达标' if vr>=P['vr_high'] else '勉强'}）；允许放量后短暂缩量整理，不破 {P['ma_stop_label']} 就持有",
            "S2": "趋势解读：高位放量滞涨 → 警惕转势",
            "S3": "趋势解读：高位放量下跌 → 破位信号，按止损线处理",
            "S4": "趋势解读：温和上涨 = 趋势健康延续，持股",
            "S5": "趋势解读：中继整理，持股观望等方向，不做T，不被震荡洗出去",
            "S6": f"趋势解读：温和下跌 → 不破 {P['ma_stop_label']} 就看是否出现企稳信号；出现可低吸",
            "S7": "趋势解读【最健康】：主力锁筹，持股待涨，不追高加仓",
            "S8": "趋势解读：缩量整理 = 筹码稳定，持有",
            "S9": f"趋势解读【核心低吸场景】：上升趋势 + 回撤≤{tol}% + 不破MA20 + 企稳信号 → 分批低吸",
            "S10": "趋势解读【底部观察】：底部区域重点观察，等首次量增价涨做右侧确认",
        }
    s_note_mode = read_map.get(s_code, "")

    # Step3 阶段
    tp_short = trend_tag.split("(")[0]
    rising = "上升" in tp_short
    falling = "下跌" in tp_short
    pos_bucket = pos_qual
    stage_code = ""; stage_name = ""; stage_note = ""; stage_basis = ""

    vr_20_max = 1.0
    if N >= 20:
        for k in range(N-20, N-1):
            base_ = sum(vols[k-5:k]) / 5 if k >= 5 else 1
            vk = vols[k] / base_ if base_ > 0 else 1
            if vk > vr_20_max: vr_20_max = vk

    if (not rising) and (s_code in ("S10","S8","S6")) and (small_body or stab_signal or drawdown_from_high <= 12):
        stage_code="A"; stage_name="A.筑底观察期"
        stage_note = "底部量缩价跌后观察，等首次量增价涨做右侧确认" if mode=="trend" else "波段不抄底，直接跳过A阶段"
        stage_basis = "(位置非高位) + 量缩/价跌 + 实体变小/企稳"
    elif (s_code=="S1") and (vr >= P["vr_high"]):
        if mode == "band":
            ok_break = (vr >= 2.0) and (last_close >= max(closes[-20:]))
            if ok_break:
                stage_code="B"; stage_name="B.爆量启动(波段)"
                stage_note="VR≥2 + 收盘创20日新高 = 短线热钱确认，建仓20~30%"
                stage_basis="B1 VR≥2.0 且收盘创20日新高(波段爆量突破)"
        else:
            if last_close >= max(closes[-20:]) * 0.98:
                stage_code="B"; stage_name="B.启动突破期(趋势)"
                stage_note="VR≥1.5 + 放量突破20日平台 → 建仓30~50%，分批金字塔可加"
                stage_basis="B1 VR≥1.5 且放量突破平台"
    elif rising and s_code in ("S1","S7","S4") and mode == "trend" and not stage_code:
        stage_code="C"; stage_name="C.主升拉升期(趋势)"
        stage_note="VR≥1.5量增价涨=主升浪 / ③量缩价涨=锁筹，持股不破MA20不走"
        stage_basis="上升趋势 + ①量增/③量缩/温和上涨沿MA20"
    elif rising and s_code == "S9" and pullback_ok and no_break_support and mode == "trend" and not stage_code:
        stage_code="D"; stage_name="D.洗盘中继期(趋势低吸)"
        stage_note=f"上升趋势 + 缩量回调（回撤{drawdown_from_high:.1f}% ≤8%）+ 不破MA20 + 企稳{'✅有' if stab_signal else '❌无'} → 分批低吸 20~30%"
        stage_basis="B2 回踩不破 + 企稳"
    elif mode == "band" and s_code == "S9" and not stage_code:
        if shrink_days <= 3 and band_reverse:
            stage_code="B"; stage_name="B.反包启动(波段)"
            stage_note="缩量回调≤3天 + 放量反包阳(VR≥2) → B2建仓20%"
            stage_basis="B2 量缩价跌次日放量反包阳(波段唯一例外的'洗盘后接回')"
        else:
            stage_code="X"; stage_name="X.波段观望(热度退潮)"
            stage_note=f"{'缩量回调已' + str(shrink_days) + '天，>3天就不陪主力洗盘，换股' if shrink_days>3 else '缩量回调但无放量反包，波段默认不参与洗盘，换股效率更高'}"
            stage_basis="波段纪律：缩量=热度退潮→不接"
    elif pos_bucket == "高" and not stage_code:
        diverge = (last_close >= max(closes[-20:])) and (vr < vr_20_max * 0.8)
        huge = vr >= P["huge_vr"]
        long_shadow = upper_shadow >= 2 * body_abs
        if huge or diverge or long_shadow or s_code == "S2":
            stage_code="E"; stage_name="E.高位派发期⚠️"
            extra = []
            if mode == "trend":
                if vr >= P["huge_vr"] and pct >= 5: extra.append("巨量长阳→清仓")
                if diverge: extra.append("量价背离→减1/3~1/2")
                if long_shadow: extra.append("长上影→减仓")
            else:
                if huge: extra.append("VR≥"+str(P["huge_vr"])+" → 当日减1/2以上")
                if diverge: extra.append("量价背离→短线借放量出货")
                if long_shadow: extra.append("长上影→减仓")
            stage_note = ("趋势：" if mode=="trend" else "波段：") + ("、".join(extra) if extra else "高位风险→减仓")
            stage_basis=f"VR≥{P['huge_vr']}({'✅' if huge else '❌'}) / 量价背离({'✅' if diverge else '❌'}) / 长上影({'✅' if long_shadow else '❌'}) / 放量滞涨(S2)"
    elif falling and s_code in ("S8","S4","S7","S10") and not stage_code:
        stage_code="F"; stage_name="F.下跌中继期"
        stage_note="趋势：缩量只是下跌中继，不破位不抄底 / 波段：直接跳过，不做左侧"
        stage_basis="下跌趋势 + 缩量反弹/横盘"
    elif s_code == "S5" and not stage_code:
        if mode == "band":
            stage_code="G"; stage_name="G.方向选择期(波段=区间操作主战场)"
            stage_note="跌至区间下沿企稳低吸，涨至上沿无量则兑现；放量突破上沿→跟单"
            stage_basis="§三④：波段主战场=区间操作"
        else:
            stage_code="G"; stage_name="G.方向选择期(趋势=中继整理)"
            stage_note="持股观望，不做T，不被震荡洗出去"
            stage_basis="§三④：趋势=中继整理"
    if not stage_code:
        if   rising:  stage_code="C"; stage_name="C.过渡-上升途中"; stage_note="暂无明确信号，持股/观望"; stage_basis="上升兜底"
        elif falling: stage_code="F"; stage_name="F.过渡-下跌途中"; stage_note="趋势/波段都不接左侧"; stage_basis="下跌兜底"
        else:         stage_code="G"; stage_name="G.过渡-震荡"; stage_note="等方向"; stage_basis="震荡兜底"

    # Step4 操作信号
    signals = []
    if mode == "band":
        if vr >= 2.0 and last_close >= max(closes[-20:]):
            signals.append({"type":"建仓","code":"B1","name":"波段·爆量突破","strength":"20%~30%",
                            "desc":f"VR={vr:.1f}≥2.0 + 收盘{last_close:.2f}创20日新高"})
        if band_reverse:
            signals.append({"type":"建仓","code":"B2","name":"波段·放量反包","strength":"20%",
                            "desc":f"昨量缩价跌 + 今放量反包(VR={vr:.1f}≥2.0)，吞没昨日实体"})
        stop_ok = True
        if stop_ma and stop_ma > 0:
            if last_close < stop_ma * (1 - P["stop_below_ma_pct"]/100):
                signals.append({"type":"清仓","code":"S1","name":"波段·跌破MA10","strength":"清仓止损",
                                "desc":f"收盘跌破{P['ma_stop_label']} {P['stop_below_ma_pct']}%以上"})
                stop_ok = False
        if vr >= P["huge_vr"] and pct >= 5:
            signals.append({"type":"减仓","code":"S2","name":"波段·巨量长阳","strength":"≥1/2减仓",
                            "desc":f"VR={vr:.1f}≥{P['huge_vr']} + 涨幅{pct:+.1f}%≥5% → 任何位置都先兑现一半以上"})
        if vr >= P["vr_high"] and (pct < 0.5 or upper_shadow >= 2*body_abs or not is_bull):
            signals.append({"type":"减仓","code":"S3","name":"波段·放量滞涨","strength":"减仓",
                            "desc":f"VR={vr:.1f}≥{P['vr_high']} 但涨幅{pct:+.1f}%<0.5% 或长上影 → 假突破/诱多"})
        if N >= 5 and closes[-5] > 0:
            chg5 = (closes[-1] / closes[-5] - 1) * 100
            if chg5 < 3:
                signals.append({"type":"观望","code":"S4","name":"波段·时间止损预警","strength":"若持仓5日仍<3%则离场",
                                "desc":f"近5交易日累计涨幅={chg5:+.1f}%<3% → 资金趴在死水，考虑离场（若已持仓5天）"})
        if len(closes)>=10 and closes[-10]>0:
            gain_proxy = (last_close / closes[-10] - 1) * 100
            lag_sign = (vr < vr_prev) or (pct < 0.8) or s_code in ("S5","S8")
            if gain_proxy >= 10 and lag_sign:
                signals.append({"type":"减仓","code":"S5","name":"波段·浮盈达标+滞涨","strength":"分批兑现",
                                "desc":f"近10日涨幅≈{gain_proxy:+.1f}%≥10% 且当前缩量/小涨/整理 → 分批兑现落袋"})
        if stop_ok and (vr_prev < 1.5 or vr >= 0.8) and not any(s["type"] in ("清仓","减仓") for s in signals) and stop_ma and last_close >= stop_ma:
            signals.append({"type":"持有","code":"H1","name":"波段·持有条件满足","strength":"—",
                            "desc":f"收盘≥MA10({stop_ma:.2f}) 且 量能无一日游 且 未触发卖出"})
    else:
        if vr >= 1.5 and s_code == "S1" and last_close >= max(closes[-20:]) * 0.98:
            signals.append({"type":"建仓","code":"B1","name":"趋势·放量突破平台","strength":"30%~50%",
                            "desc":f"VR={vr:.1f}≥1.5 + 收盘{last_close:.2f}站上20日平台高点×0.98"})
        if rising and s_code == "S9" and pullback_ok and no_break_support and stab_signal:
            signals.append({"type":"建仓","code":"B2","name":"趋势·缩量回调低吸","strength":"20%~30%",
                            "desc":f"回撤{drawdown_from_high:.1f}%≤8% + 不破MA20 + 企稳 → 分批低吸"})
        low_region = (gain_from_bottom < 30) or (pos_1y < 40)
        if low_region and s_code == "S1" and vr >= 1.5:
            signals.append({"type":"建仓","code":"B3","name":"趋势·筑底后首次放量","strength":"试仓",
                            "desc":f"量缩后VR={vr:.1f}≥1.5 + 价涨 = 反转确认，试仓"})
        if stop_ma and stop_ma > 0 and last_close >= stop_ma and drawdown_from_high <= P["pullback_tol"] and not any(s["type"]=="清仓" for s in signals):
            signals.append({"type":"持有","code":"H1","name":"趋势·收盘未破MA20","strength":"持股",
                            "desc":f"收盘在{P['ma_stop_label']}({stop_ma:.2f})上方，容忍回撤{P['pullback_tol']}%内，不因单日阴线离场"})
        if rising and stop_ma and (0 <= last_close - stop_ma <= stop_ma*0.02) and stab_signal and vr >= 1.5:
            signals.append({"type":"加仓","code":"A1","name":"趋势·回踩MA20再度放量","strength":"+加仓",
                            "desc":f"回踩{stop_ma:.2f}企稳 + VR={vr:.1f}≥1.5再度放量上涨 → 加仓"})
        if stop_ma and stop_ma > 0:
            if last_close < stop_ma * (1 - P["stop_below_ma_pct"]/100):
                signals.append({"type":"清仓","code":"S1","name":"趋势·首次破MA20","strength":"清仓(空间止损)",
                                "desc":f"收盘跌破{P['ma_stop_label']} {P['stop_below_ma_pct']}%以上 → 用空间止损，不用时间止损"})
        if pos_bucket == "高" and vr >= 3.0 and pct >= 5:
            signals.append({"type":"清仓","code":"S2","name":"趋势·高位巨量长阳","strength":"清仓",
                            "desc":f"位置=高位 + VR={vr:.1f}≥3.0 + 涨幅{pct:+.1f}%≥5% → 清仓"})
        if pos_bucket == "高" and last_close >= max(closes[-20:]) and vr < vr_20_max * 0.8:
            signals.append({"type":"减仓","code":"S3","name":"趋势·高位量价背离","strength":"减1/3~1/2",
                            "desc":f"股价创20日新高，但VR={vr:.1f} < 前20日最大VR {vr_20_max:.1f}×80% → 量能不支"})

    # Step5 风控
    vetoes = []
    if falling and s_code == "S10":
        vetoes.append({"code":"V1","name":"下跌趋势缩量不抄底","desc":"MA空头排列 + 量缩价跌 → 禁止建仓"})
    if pos_bucket == "高" and vr >= P["high_buy_fobid_vr"]:
        vetoes.append({"code":"V2","name":"高位放量防出货",
                       "desc":f"位置=高位 + VR={vr:.1f}≥{P['high_buy_fobid_vr']} → 禁止追买（{'波段阈值更严1.8' if mode=='band' else '趋势阈值2.0'}）"})
    if vr >= 1.5 and pct < 0.5:
        vetoes.append({"code":"V3","name":"放量不涨=诱多",
                       "desc":f"VR≥1.5 且当日涨幅{pct:+.1f}%<0.5% → 撤销买入计划"})
    if one_day_flash:
        if mode == "band":
            vetoes.append({"code":"V4","name":"量能一日游→波段直接离场",
                           "desc":f"前VR={vr_prev:.1f}放量→今VR={vr:.2f}<0.8 → §六：波段当日必须走完，不隔夜侥幸"})
        else:
            vetoes.append({"code":"V4","name":"量能一日游→趋势信号作废观望",
                           "desc":f"前VR={vr_prev:.1f}放量→今VR={vr:.2f}<0.8 → §六：转为观望，不买"})
    neg_slope = False
    if ok_ma and N >= 4 and ma20[-4] > 0:
        neg_slope = (ma20[-1] / ma20[-4] - 1) * 100 < -0.05
    if neg_slope and falling:
        if mode == "band":
            vetoes.append({"code":"V5","name":"逆势不操作→波段空仓","desc":"§六：(个股MA20斜率向下 + 下跌趋势) → 波段不做，空仓"})
        else:
            vetoes.append({"code":"V5","name":"逆势不操作→趋势仓位减半","desc":"§六：指数/个股逆势 → 趋势仓位减半（或放弃）"})
    if s_code == "S10" and not stab_signal:
        vetoes.append({"code":"V6","name":"量缩价跌≠马上反转",
                       "desc":"无止跌阳/阳包阴/长下影企稳信号前，只观察不买入"})
    if (not no_break_support) and (not close_back):
        if mode == "band":
            vetoes.append({"code":"V7","name":"破位必走→波段当日走完","desc":f"收盘跌破支撑位{support:.2f} 2%以上 → §六：波段当日必须走完，不隔夜侥幸，不补仓摊低"})
        else:
            vetoes.append({"code":"V7","name":"破位必走→趋势即清不补","desc":f"收盘跌破支撑位{support:.2f} 2%以上 → §六：趋势首次破位即清，不补仓摊低"})
    if len(vetoes) >= 3 and any(v["code"]=="V7" for v in vetoes):
        vetoes.append({"code":"·","name":("波段" if mode=="band" else "趋势")+"黑名单预警",
                       "desc":"§六：反复触发止损后 → 近期降级观察 / 不碰（由交易员实际记录止损次数）"})

    # Step6 盘后流程
    discip = "不放量不拿，让资金周转" if mode=="band" else "不破位不走，让利润奔跑"
    post_flow = [
        ("① 算三个数",   f"{P['name']}: VR量比={vr:.2f}（放≥{P['vr_high']}/缩≤{P['vr_low']}） | 当日涨跌幅={pct:+.2f}%（阈值±{up_thresh:+.1f}% σ={sigma:.2f}%） | 距250日低点={gain_from_bottom:+.1f}% / 近1年分位={pos_1y:.0f}%"),
        ("② 定量价态",   f"量={vol_tag} | 价={price_tag} → {s_code} {s_name}；{s_note_mode}"),
        ("③ 定趋势+位置", (f"趋势={trend_tag} | 位置={pos_tag} | 回撤={drawdown_from_high:.1f}%（阈值≤{P['pullback_tol']}%）| 锚定{P['ma_anchor_label']}={anchor_ma:.2f}") if anchor_ma else f"趋势={trend_tag} | 位置={pos_tag}"),
        ("④ 定阶段",     f"{stage_name} — {stage_note}（依据：{stage_basis}）"),
        ("⑤ 过风控§六",   (("全过 ✓" if not vetoes else "；".join(v["code"]+v["name"] for v in vetoes[:2])) + f" · 共{len(vetoes)}条") + f"；纪律：'{discip}'"),
        ("⑥ 出指令("+P["name"]+")",  ", ".join(f"{s['type']}{s['code']} {s['name']} ({s['strength']})" for s in signals[:3]) if signals else "无操作信号 · "+("换股/等放量" if mode=="band" else "观望持股")),
    ]

    # ---- 动态调整持仓周期与仓位（基于个股波动率σ + 趋势强度MA20斜率）----
    ma20_slope = 0.0
    if ok_ma and N >= 4 and not math.isnan(ma20[-4]) and ma20[-4] > 0:
        ma20_slope = (ma20[-1] / ma20[-4] - 1) * 100
    if mode == "band":
        if sigma >= 3.0:
            dyn_hold = "3~8 个交易日"; dyn_size = "15%~25%"
            dyn_reason = f"高波动(σ={sigma:.1f}%)→缩短持仓+降仓位控风险"
        elif sigma >= 1.5:
            dyn_hold = "3~15 个交易日"; dyn_size = "20%~30%"
            dyn_reason = f"中波动(σ={sigma:.1f}%)→标准波段周期"
        else:
            dyn_hold = "5~15 个交易日"; dyn_size = "25%~35%"
            dyn_reason = f"低波动(σ={sigma:.1f}%)→弹性不足，延长等脉冲"
    else:
        if ma20_slope >= 2.0:
            dyn_hold = "2~6 个月"; dyn_size = "35%~50%"
            dyn_reason = f"强趋势(MA20斜率{ma20_slope:+.1f}%)→可拿久+加仓"
        elif ma20_slope >= 0:
            dyn_hold = "1~6 个月"; dyn_size = "30%~50%"
            dyn_reason = f"中趋势(MA20斜率{ma20_slope:+.1f}%)→标准趋势持仓"
        else:
            dyn_hold = "1~3 个月"; dyn_size = "25%~40%"
            dyn_reason = f"弱趋势(MA20斜率{ma20_slope:+.1f}%)→缩短持仓+降仓位"

    return {
        "mode": mode,
        "mode_name": P["zh"],
        "hold_cycle": dyn_hold,
        "hold_cycle_base": P["hold_cycle"],
        "ma_anchor_label": P["ma_anchor_label"],
        "ma_stop_label": P["ma_stop_label"],
        "position_weakened": P["position_weakened"],
        "single_size": dyn_size,
        "single_size_base": P["position_single_size"],
        "attitude_shrink": P["attitude_shrink"],
        "dyn_reason": dyn_reason,
        "params": {
            "vr": round(vr, 2), "vr_high_thresh": P["vr_high"], "vr_low_thresh": P["vr_low"],
            "huge_vr_thresh": P["huge_vr"], "high_buy_fobid_vr": P["high_buy_fobid_vr"],
            "pct": round(pct, 2), "up_thresh": round(up_thresh, 2), "down_thresh": round(down_thresh, 2),
            "sigma": round(sigma, 2),
            "gain_from_bottom": round(gain_from_bottom, 1),
            "pos_1y": round(pos_1y, 1),
            "support": round(support, 2),
            "stop_loss_price": round(stop_loss_price, 2) if stop_loss_price else None,
            "drawdown_from_high": round(drawdown_from_high, 2),
            "drawdown_tol": P["pullback_tol"],
            "stop_below_ma_pct": P["stop_below_ma_pct"],
            "shrink_days": shrink_days,
            "vr_prev": round(vr_prev, 2),
            "no_break_support": bool(no_break_support),
            "one_day_flash": bool(one_day_flash),
            "band_reverse": bool(band_reverse),
            "stab_signal": bool(stab_signal),
        },
        "tags": tags,
        "state": {"code": s_code, "name": s_name, "note_trend": s_note_mode},
        "stage": {"code": stage_code, "name": stage_name, "note": stage_note, "basis": stage_basis},
        "signals": signals,
        "vetoes": vetoes,
        "post_flow": post_flow,
    }


def calc_vp_system(bars: list, closes: list = None,
                   highs: list = None, lows: list = None,
                   vols: list = None, chgs: list = None,
                   ma5: list = None, ma10: list = None, ma20: list = None,
                   my_style: dict = None) -> dict:
    """【双模式版】量价关系量化体系：趋势票 × 波段票 分开跑两套规则。
    返回 {style, style_code, trend_score, swing_score, dominant, trend, band, glossary}。
    my_style: 可选，传入 analyze_buy_sell 的完整版风格判定结果，复用其 trend_score/swing_score/style_type，保证全站风格分数一致。
    """
    import statistics
    if closes is None: closes = [b["close"] for b in bars]
    if highs is None:  highs  = [b["high"]  for b in bars]
    if lows is None:   lows   = [b["low"]   for b in bars]
    if vols is None:   vols   = [b["volume"] for b in bars]
    if chgs is None:   chgs = daily_changes(bars)
    if ma5  is None: ma5  = sma(closes, 5)
    if ma10 is None: ma10 = sma(closes, 10)
    if ma20 is None: ma20 = sma(closes, 20)
    if len(bars) < 30:
        return {}
    # 优先复用完整版风格判定（与"我的持仓风格"卡同源），保证全站分数一致
    if my_style and isinstance(my_style, dict) and "trend_score" in my_style:
        t_score = float(my_style.get("trend_score", 50))
        s_score = float(my_style.get("swing_score", 50))
        sc = my_style.get("style_type", "") or ""
        if "波段" in sc and "趋势" not in sc:
            style_code, style_label = "band", "波段票 · 不放量不拿"
        elif "趋势" in sc and "波段" not in sc:
            style_code, style_label = "trend", "趋势票 · 不破位不走"
        elif "偏波段" in sc:
            style_code, style_label = "band", "波段票 · 不放量不拿"
        elif "偏趋势" in sc:
            style_code, style_label = "trend", "趋势票 · 不破位不走"
        elif t_score >= s_score:
            style_code, style_label = "trend", "趋势票 · 不破位不走"
        else:
            style_code, style_label = "band", "波段票 · 不放量不拿"
    else:
        style_label, style_code, t_score, s_score = _classify_style_light(closes, ma5, ma10, ma20, highs, lows)
    trend_r = _vp_single(bars, closes, highs, lows, vols, chgs, ma5, ma10, ma20, mode="trend")
    band_r  = _vp_single(bars, closes, highs, lows, vols, chgs, ma5, ma10, ma20, mode="band")
    dominant = style_code if style_code in ("trend","band") else ("trend" if t_score >= s_score else "band")

    glossary = {
        "params_table": [
            {"维度": "持股周期",           "波段票": "3~15 个交易日", "趋势票": "1~6 个月"},
            {"维度": "均线体系",           "波段票": "MA5/MA10/MA20，锚定 MA10", "趋势票": "MA10/MA20/MA60，锚定 MA20"},
            {"维度": "放量阈值（量比 VR）",  "波段票": "≥ 2.0（要爆量，确认短线热钱）", "趋势票": "≥ 1.5"},
            {"维度": "缩量阈值",           "波段票": "≤ 0.7", "趋势票": "≤ 0.8"},
            {"维度": "回调容忍（自20日高点回撤）","波段票": "≤ 5%（深了弹性就没了）", "趋势票": "≤ 8%"},
            {"维度": "破位止损线",         "波段票": "收盘跌破 MA10 超 1.5%", "趋势票": "收盘跌破 MA20 超 2%"},
            {"维度": "巨量阈值",           "波段票": "≥ 2.5", "趋势票": "≥ 3.0"},
            {"维度": "高位禁买量比",       "波段票": "≥ 1.8", "趋势票": "≥ 2.0"},
            {"维度": "位置分档",           "波段票": "弱化，更看区间支撑/压力位", "趋势票": "保留，是阶段判断核心"},
            {"维度": "单次建仓仓位",       "波段票": "20%~30%（试错成本要低）", "趋势票": "30%~50%（可分批金字塔加仓）"},
        ],
        "five_states": [
            {"状态": "① 量增价涨",
             "波段票": "VR必须≥2.0才算有效启动；跟进后次日VR掉回0.8以下=脉冲失败，当天离场，不观望",
             "趋势票": "VR≥1.5 + 突破平台即可跟进；允许放量后短暂缩量整理，只要不破MA20就持有"},
            {"状态": "② 缩量回调（分歧最大）",
             "波段票": "默认不参与洗盘。缩量=短线热度退潮，资金效率为王。唯一例外：缩量回调≤3天 + 今日立刻出现放量反包阳（今日阳线实体完全吞没昨日阴线实体且VR≥2）→ 快速跟一笔；否则换股。波段被洗出来是常态，宁可错过不可套住。",
             "趋势票": "核心低吸场景。上升趋势 + 回撤≤8% + 不破MA20 + 出现企稳信号（止跌阳/阳包阴/下影线≥2倍实体）→ 分批低吸。"},
            {"状态": "③ 量缩价涨",
             "波段票": "警惕。波段要的是持续放量推升，高位量缩价涨=动能衰减，不加仓，并把止盈位上移到MA10；一旦滞涨立即兑现。",
             "趋势票": "主力锁筹，最健康的上涨，持股待涨，不追高加仓。"},
            {"状态": "④ 量平价平",
             "波段票": "波段主战场——区间操作：跌至区间下沿（支撑位）企稳低吸，涨至区间上沿（压力位）无量则兑现；放量突破上沿→转成突破单跟进。",
             "趋势票": "中继整理，持股观望等方向，不做T、不被震荡洗出去。"},
            {"状态": "⑤ 量缩价跌",
             "波段票": "直接跳过，波段永远不抄底，只做右侧启动。",
             "趋势票": "底部区域重点观察，等「首次量增价涨」做右侧确认。"},
        ],
        "band_rules": {
            "buy": ["B1：VR ≥ 2.0 且收盘创 20 日新高（爆量突破）→ 建仓 20%~30%",
                    "B2：昨日量缩价跌 + 今日放量反包阳（吞没昨日实体且 VR ≥ 2.0）→ 建仓 20%"],
            "hold": ["H1：收盘价 ≥ MA10",
                     "H2：量能未出现「一日游」（放量次日 VR ≥ 0.8）",
                     "H3：未触发任何卖出条件"],
            "sell": ["S1：收盘跌破 MA10 超 1.5% → 清仓（止损）",
                     "S2：巨量长阳（VR ≥ 2.5 且涨幅 ≥ 5%，任何位置）→ 当日减仓一半以上",
                     "S3：放量滞涨（VR ≥ 2.0 但涨幅 < 0.5% 或 长上影）→ 减仓",
                     "S4：时间止损——买入后 5 个交易日涨幅 < 3% → 离场（资金效率）",
                     "S5：浮盈 ≥ 10%~15% 且滞涨 → 分批兑现"],
        },
        "trend_rules": {
            "buy": ["B1：VR ≥ 1.5 且放量突破 20 日平台 → 建仓 30%~50%",
                    "B2：上升趋势中缩量回调（回撤 ≤8%、不破 MA20、企稳）→ 分批低吸 20%~30%",
                    "B3：底部量缩价跌后首次量增价涨 → 试仓"],
            "hold": ["H1：收盘在 MA20 上方即持有，容忍 8% 以内回撤，不因单日阴线离场"],
            "add":  ["A1：回踩 MA20 企稳后再度放量上涨 → 加仓"],
            "sell": ["S1：首次收盘跌破 MA20 超 2% → 清仓（用空间止损，不用时间止损）",
                     "S2：高位巨量长阳（VR ≥ 3.0 且涨幅 ≥ 5%）→ 清仓",
                     "S3：高位量价背离（创 20 日新高但 VR 不足前 20 日最大 VR × 80%）→ 减 1/3~1/2"],
        },
        "risk_diff": [
            {"规则":"量能一日游（放量次日 VR＜0.8）",
             "波段票":"§六：直接离场（波段资金不能趴着）",
             "趋势票":"信号作废，转为观望"},
            {"规则":"大盘/个股逆势（MA20斜率向下 + 下跌趋势）",
             "波段票":"§六：不做，空仓",
             "趋势票":"仓位减半，可做"},
            {"规则":"跌穿止损线后处理",
             "波段票":"§六：当日必须走完，不隔夜侥幸，不补仓摊低",
             "趋势票":"§六：首次破位即清，不补仓摊低"},
            {"规则":"同一标的反复触发止损 ≥ 2 次",
             "波段票":"§六：拉黑名单，近期不再碰",
             "趋势票":"§六：降级为观察，等重新站回 MA20"},
        ],
        "discipline": {
            "trend": "不破位不走，让利润奔跑 —— 看「空间」：一个缩量回调只看「破了 MA20 没有」。",
            "band":  "不放量不拿，让资金周转 —— 看「时间」：一个缩量回调要问「3 天内能不能重新放量」。",
            "tag": "趋势票 VS 波段票：赚的不是同一段钱。趋势票拿 1~6 个月主升浪的钱；波段票赚 3~15 天弹性脉冲的钱。",
        },
    }

    # ---- 后向兼容：占优模式结果平铺到顶层 (供 analyze_buy_sell 和 旧前端渲染兼容) ----
    dom_v = trend_r if dominant == "trend" else band_r
    return {
        "style": style_label,
        "style_code": style_code,
        "trend_score": round(t_score, 1),
        "swing_score": round(s_score, 1),
        "dominant": dominant,
        "trend": trend_r,
        "band":  band_r,
        "glossary": glossary,
        # ---- 顶层平铺 (占优模式)：兼容旧调用点 analysis["item=量价关系"] ----
        "params":  dom_v.get("params", {}),
        "tags":    dom_v.get("tags", {}),
        "state":   dom_v.get("state", {}),
        "stage":   dom_v.get("stage", {}),
        "signals": dom_v.get("signals", []),
        "vetoes":  dom_v.get("vetoes", []),
        "post_flow": dom_v.get("post_flow", []),
        # 另外一份：另一模式的平铺，方便前端快速访问
        "other":   (band_r if dominant == "trend" else trend_r),
    }


def calc_kdj_system(bars: list, closes: list, highs: list, lows: list,
                    k: list, d: list, j: list,
                    ma5: list, ma10: list, ma20: list,
                    wk: list, wd: list, wj: list,
                    vp_system: dict = None) -> dict:
    """【双模式版】KDJ操作手册：波段票(主武器) × 趋势票(扳机) + 周KDJ辅助 + 左侧抄底
    返回 {style_code, dominant, band, trend, weekly, left_side, glossary}
    """
    N = len(closes)
    if N < 30 or math.isnan(j[-1]):
        return {}
    # 复用量价系统的风格判定
    vp = vp_system or {}
    style_code = vp.get("style_code") or "mix"
    dominant = vp.get("dominant") or ("trend" if style_code == "trend" else "band")

    # ---- 通用KDJ状态 ----
    K_t, D_t, J_t = k[-1], d[-1], j[-1]
    K_y, D_y, J_y = (k[-2], d[-2], j[-2]) if N >= 2 and not math.isnan(k[-2]) else (K_t, D_t, J_t)
    K_3, J_3 = (k[-3], j[-3]) if N >= 3 and not math.isnan(k[-3]) else (K_t, J_t)
    # 金叉/死叉判定 (当日K穿越D)
    golden_cross = (K_y <= D_y) and (K_t > D_t)
    death_cross  = (K_y >= D_y) and (K_t < D_t)
    # 金叉位置分档
    if golden_cross:
        if K_t < 30:   cross_pos = "低位金叉"
        elif K_t <= 60: cross_pos = "中位金叉"
        else:           cross_pos = "高位金叉"
    else:
        cross_pos = ""
    # 钝化判定
    high_blunt = all(not math.isnan(k[i]) and k[i] >= 80 for i in range(N-3, N)) if N >= 3 else False
    low_blunt  = all(not math.isnan(k[i]) and k[i] <= 20 for i in range(N-3, N)) if N >= 3 else False
    # J值拐头
    j_turn_down = (J_t < J_y) and (J_y >= 100 or J_y > J_3)
    j_single_drop = (J_y - J_t) > 15
    j_extreme_high = J_t > 100 or J_y > 100
    j_extreme_low = J_t < 0

    # ---- 背离判定 (20日窗口) ----
    if N >= 25:
        idx_20_high = -1; peak_j_at_high = J_t
        for i in range(N-20, N):
            if highs[i] == max(highs[N-20:]):
                idx_20_high = i; peak_j_at_high = j[i]; break
        prev_peak_idx = -1; prev_peak_j = J_t
        for i in range(max(0, idx_20_high-20), idx_20_high):
            if highs[i] >= max(highs[max(0,idx_20_high-20):idx_20_high]) * 0.99 and not math.isnan(j[i]):
                if j[i] > prev_peak_j: prev_peak_j = j[i]; prev_peak_idx = i
        top_diverge = (idx_20_high > 0 and prev_peak_idx >= 0
                       and closes[-1] >= max(closes[-20:]) * 0.99
                       and peak_j_at_high < prev_peak_j - 10)
        idx_20_low = -1; trough_j_at_low = J_t
        for i in range(N-20, N):
            if lows[i] == min(lows[N-20:]):
                idx_20_low = i; trough_j_at_low = j[i]; break
        prev_trough_idx = -1; prev_trough_j = J_t
        for i in range(max(0, idx_20_low-20), idx_20_low):
            if lows[i] <= min(lows[max(0,idx_20_low-20):idx_20_low]) * 1.01 and not math.isnan(j[i]):
                if j[i] < prev_trough_j: prev_trough_j = j[i]; prev_trough_idx = i
        bottom_diverge = (idx_20_low > 0 and prev_trough_idx >= 0
                          and closes[-1] <= min(closes[-20:]) * 1.01
                          and trough_j_at_low > prev_trough_j + 5)
    else:
        top_diverge = False; bottom_diverge = False

    # ---- 量能联动 (复用vp的vr) ----
    dom_v = vp.get("trend") if dominant == "trend" else vp.get("band")
    vr = dom_v.get("params", {}).get("vr", 1.0) if dom_v else 1.0
    vp_state_code = dom_v.get("state", {}).get("code", "") if dom_v else ""
    # 上升趋势+缩量回调(量价D阶段)判定
    rising = bool(dom_v and "上升" in (dom_v.get("tags", {}).get("trend") or ""))
    pullback_shrink = rising and vp_state_code == "S9"

    # ---- 周KDJ ----
    wK_t, wD_t, wJ_t = 50.0, 50.0, 50.0
    w_golden = w_death = False
    if wj and not math.isnan(wj[-1]):
        wK_t, wD_t, wJ_t = wk[-1], wd[-1], wj[-1]
        if len(wk) >= 2 and not math.isnan(wk[-2]) and not math.isnan(wd[-2]):
            w_golden = (wk[-2] <= wd[-2]) and (wK_t > wD_t)
            w_death  = (wk[-2] >= wd[-2]) and (wK_t < wD_t)
    wJ_low = wJ_t < 30; wJ_high = wJ_t > 80

    ok_ma = (not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) and not math.isnan(ma20[-1]) and ma20[-1] > 0)
    last_close = closes[-1]
    above_ma5 = ok_ma and last_close > ma5[-1]
    above_ma10 = ok_ma and last_close > ma10[-1]
    above_ma20 = ok_ma and last_close > ma20[-1]

    # ====================================================================
    # 波段票KDJ (主武器)
    # ====================================================================
    band_signals = []
    band_tags = {
        "K": f"{K_t:.1f}（{'超买≥80' if K_t>=80 else '超卖≤20' if K_t<=20 else '中性'}）",
        "D": f"{D_t:.1f}",
        "J": f"{J_t:.1f}（{'极端超买>100' if J_t>100 else '极端超卖<0' if J_t<0 else '正常'}）",
        "cross": f"{'✅'+cross_pos if golden_cross else '⚠️高位死叉' if death_cross and K_t>80 else '无叉'}",
        "blunt": f"{'⚠️高位钝化(K≥80连续3日)' if high_blunt else '⚠️低位钝化' if low_blunt else '无钝化'}",
        "diverge": f"{'⚠️顶背离(J峰值低≥10点)' if top_diverge else '底背离' if bottom_diverge else '无背离'}",
        "weekly": f"周J={wJ_t:.0f}（{'低位' if wJ_low else '高位超买' if wJ_high else '中性'}）{'周金叉✅' if w_golden else '周死叉⚠️' if w_death else ''}",
    }
    # B1: 低位金叉+双确认
    if golden_cross and K_t < 30 and above_ma5:
        band_signals.append({"type":"建仓","code":"B1","name":"波段·低位金叉+双确认","strength":"20%",
            "desc":f"K={K_t:.1f}<30金叉 + 收盘站上MA5 → 建仓20%"})
    # B2: 金叉+爆量共振
    if golden_cross and vr >= 2.0:
        band_signals.append({"type":"建仓","code":"B2","name":"波段·金叉爆量共振","strength":"20%~30%",
            "desc":f"金叉当日VR={vr:.1f}≥2.0(与量价手册联动) → 建仓20%~30%"})
    # B3: 区间下沿金叉 (复用量价S5/G区间 + 低位金叉)
    if golden_cross and K_t < 40 and vp_state_code in ("S5","S8"):
        band_signals.append({"type":"建仓","code":"B3","name":"波段·区间下沿金叉","strength":"20%",
            "desc":"横盘区间下沿(支撑位±2%)金叉 → 建仓20%，上沿无量兑现"})
    # 持有判定
    band_hold = (K_t > D_t) and (not (J_t < J_y and J_y < J_3)) and above_ma10
    if band_hold and not any(s["type"] in ("减仓","清仓") for s in band_signals):
        band_signals.append({"type":"持有","code":"H1","name":"波段·KDJ持有条件满足","strength":"—",
            "desc":"K>D(金叉状态未破) + J未连续2日下行 + 收盘在MA10上方"})
    # S1: 高位死叉
    if death_cross and K_t > 80:
        band_signals.append({"type":"减仓","code":"S1","name":"波段·高位死叉","strength":"减仓一半",
            "desc":f"K={K_t:.1f}>80区域K下穿D → 减仓一半"})
    # S2: J>100拐头单日回落>15
    if j_extreme_high and j_single_drop:
        band_signals.append({"type":"减仓","code":"S2","name":"波段·钝化终结(J拐头)","strength":"减仓",
            "desc":f"J曾>100后单日回落{J_y-J_t:.0f}>15点 → 钝化终结减仓"})
    # S3: 顶背离
    if top_diverge:
        band_signals.append({"type":"减仓","code":"S3","name":"波段·顶背离","strength":"减1/3~1/2",
            "desc":"价新高但J峰值比前次低≥10点 → 减仓1/3~1/2"})
    # S4: 死叉+放量下跌
    if death_cross and vr >= 2.0 and closes[-1] < bars[-1]["open"]:
        band_signals.append({"type":"清仓","code":"S4","name":"波段·死叉+放量下跌(双杀)","strength":"清仓不隔夜",
            "desc":f"死叉 + VR={vr:.1f}≥2 + 收阴 → 双杀信号清仓"})
    # S5: 跌破MA10超1.5%
    if ok_ma and last_close < ma10[-1] * 0.985:
        band_signals.append({"type":"清仓","code":"S5","name":"波段·跌破MA10止损","strength":"清仓",
            "desc":f"收盘{last_close:.2f}跌破MA10({ma10[-1]:.2f})超1.5% → KDJ服从止损纪律"})
    # S6: 时间止损
    if N >= 5 and closes[-5] > 0:
        chg5 = (last_close / closes[-5] - 1) * 100
        if chg5 < 3 and not death_cross:
            band_signals.append({"type":"清仓","code":"S6","name":"波段·时间止损","strength":"离场",
                "desc":f"买入5日涨幅{chg5:+.1f}%<3%，KDJ未死叉也离场"})
    # 量能矛盾降级标记
    vol_conflict = golden_cross and vr < 1.2
    band_note = ""
    if vol_conflict:
        band_note = "⚠️量能矛盾：金叉但VR<1.2(无量金叉)→仓位减半"

    # ====================================================================
    # 趋势票KDJ (只做两件事: 回调扳机 + 高位预警)
    # ====================================================================
    trend_signals = []
    trend_tags = dict(band_tags)  # 复用基础KDJ值
    trend_tags["role"] = "KDJ只做扳机+预警，方向归均线量价"
    # 用途一: 回调买点扳机
    if pullback_shrink and golden_cross and K_t < 30:
        trend_signals.append({"type":"建仓","code":"T-B1","name":"趋势·回调扳机(MA20+低位金叉)","strength":"20%~30%",
            "desc":f"上升趋势缩量回调至MA20 + KDJ低位金叉(K={K_t:.1f}<30) → 低吸20%~30%"})
    # 用途二: 高位预警(只减仓不清仓)
    if top_diverge:
        trend_signals.append({"type":"减仓","code":"P1","name":"趋势·高位顶背离","strength":"减1/3",
            "desc":"顶背离 → 减仓1/3，清不清看MA20"})
    if high_blunt and j_single_drop:
        trend_signals.append({"type":"减仓","code":"P2","name":"趋势·钝化终结","strength":"减1/3",
            "desc":f"K连续≥3日>80钝化后，J单日回落{J_y-J_t:.0f}>15 → 减仓1/3"})
    # 趋势持有: 钝化期间不死叉不卖
    if high_blunt and not death_cross and not any(s["type"] in ("减仓","清仓") for s in trend_signals):
        trend_signals.append({"type":"持有","code":"T-H1","name":"趋势·钝化持有(不死叉不卖)","strength":"持股",
            "desc":"强势主升浪KDJ在80上方钝化，死叉=假信号，卖出只看量价三条"})
    trend_note = ""
    if death_cross and 50 < K_t < 80:
        trend_note = "ℹ️中位死叉(K 50~70)=洗盘噪音，趋势票不因它卖出"

    # ====================================================================
    # 左侧抄底模块 (仅趋势票, 四重过滤)
    # ====================================================================
    left_side = {"enable": False, "signals": [], "note": ""}
    lb = min(250, N); low_250 = min(lows[-lb:])
    gain_from_bottom = (last_close / low_250 - 1) * 100 if low_250 > 0 else 0
    low_region = gain_from_bottom < 30
    # J<0持续≥2日
    j_neg_2d = j_extreme_low and N >= 2 and not math.isnan(j[-2]) and j[-2] < 0
    # 量能衰竭
    vol_exhaust = vr < 0.7
    body_shrink = N >= 3 and all(
        abs(closes[i] - bars[i]["open"]) / max(bars[i]["open"], 1e-6) * 100 < 2
        for i in range(N-3, N)
    )
    l1 = j_neg_2d; l2 = low_region; l3 = vol_exhaust and body_shrink; l4 = bottom_diverge
    left_side["filters"] = {
        "L1_J_neg_2d": bool(l1), "L2_low_region": bool(l2),
        "L3_vol_exhaust": bool(l3), "L4_bottom_diverge": bool(l4),
    }
    if l1 and l2 and l3 and l4:
        left_side["enable"] = True
        left_side["signals"] = [
            {"step":"第一笔(试探)","trigger":"L1~L4全部满足","size":"10%~15%"},
            {"step":"第二笔(转强)","trigger":"J上拐+当日收阳线","size":"+10%"},
            {"step":"第三笔(确认)","trigger":"KDJ金叉形成","size":"加至30%"},
            {"step":"右侧接力","trigger":"放量阳线(VR≥1.5)突破MA20","size":"按趋势票规则加至正常仓位"},
        ]
        left_side["note"] = "止损:收盘价跌破抄底日最低价2%→无条件全清; 仓位上限:右侧确认前≤30%"
    elif l1 or l4:
        left_side["note"] = f"部分条件满足: J<0={l1} 底背离={l4} 底部区域={l2} 量能衰竭={l3} → 未全部满足不进场"

    # ---- 速查表 ----
    glossary = {
        "quick_ref": [
            {"场景":"K<30金叉+次日阳线+站上MA5","波段票":"建仓20%","趋势票":"回调至MA20时→低吸扳机"},
            {"场景":"金叉+VR≥2","波段票":"建仓20%~30%","趋势票":"突破确认可加仓"},
            {"场景":"K>80死叉","波段票":"减仓一半","趋势票":"无视(钝化噪音)"},
            {"场景":"死叉+放量下跌","波段票":"清仓","趋势票":"看是否破MA20，破则清"},
            {"场景":"J>100拐头回落>15点","波段票":"减仓","趋势票":"钝化终结减仓1/3"},
            {"场景":"顶背离","波段票":"减仓1/3~1/2","趋势票":"减仓1/3"},
            {"场景":"J<0两日+底背离+缩量","波段票":"不做(不抄底)","趋势票":"左侧首仓10%~15%，止损-2%"},
            {"场景":"中位金叉(30~60)","波段票":"不开新仓","趋势票":"不用"},
        ],
        "discipline": {
            "band":"波段票用KDJ赚摆动的钱——金叉进、死叉出、无量减半、破位必走",
            "trend":"趋势票用KDJ买回调的点——方向看均线量价，KDJ只是扳机",
            "left":"左侧抄底是特许动作，四重过滤、30%封顶、2%硬止损，一步不能少",
        },
    }

    return {
        "style_code": style_code,
        "dominant": dominant,
        "kdj_vals": {"K": round(K_t,1), "D": round(D_t,1), "J": round(J_t,1),
                     "weekly_J": round(wJ_t,1), "weekly_K": round(wK_t,1), "weekly_D": round(wD_t,1)},
        "cross": {"golden": golden_cross, "death": death_cross, "pos": cross_pos},
        "blunt": {"high": high_blunt, "low": low_blunt},
        "diverge": {"top": top_diverge, "bottom": bottom_diverge},
        "band": {"signals": band_signals, "tags": band_tags, "note": band_note},
        "trend": {"signals": trend_signals, "tags": trend_tags, "note": trend_note},
        "weekly": {"J": round(wJ_t,1), "golden": w_golden, "death": w_death,
                   "low": wJ_low, "high": wJ_high,
                   "note": f"周J={wJ_t:.0f} {'低位支撑' if wJ_low else '高位风险' if wJ_high else '中性'}{'，周金叉✅' if w_golden else '，周死叉⚠️' if w_death else ''}"},
        "left_side": left_side,
        "glossary": glossary,
    }





def analyze_buy_sell(bars: list[dict]) -> dict:
    """
    基于均线趋势的买卖点分析 (以20日线为核心)。
    返回: {signal, signal_type, entry_price, stop_loss, target_price,
           reason, trend, alignment, vol_status, actions}
    signal: "buy" / "sell" / "wait"
    """
    if len(bars) < 30:
        return {"signal": "wait", "signal_type": "数据不足", "reason": "K线不足30根，无法分析"}
    closes = [b["close"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    vols = [b["volume"] for b in bars]
    opens = [b["open"] for b in bars]
    c = closes[-1]

    ma5 = sma(closes, 5)
    ma10 = sma(closes, 10)
    ma20 = sma(closes, 20)
    ma60 = sma(closes, 60)

    # ============ 近半年股性分析 → 多档买点(数据特征提取部分, 价格计算与desc在return前拼接) ============
    # --- 特征提取: ATR / 回调分位数 / 支撑位 / MA20回踩统计 ---
    def _quantile(arr, q):
        """简单分位数 (0<=q<=1), 忽略NaN"""
        xs = sorted([x for x in arr if x is not None and not (isinstance(x, float) and math.isnan(x))])
        if not xs:
            return 0.0
        if len(xs) == 1:
            return xs[0]
        pos = (len(xs) - 1) * q
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return xs[lo]
        return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)

    n_use = min(120, len(closes))
    c_start = len(closes) - n_use
    cl120 = closes[c_start:]
    hi120 = highs[c_start:]
    lo120 = lows[c_start:]
    op120 = opens[c_start:]

    atr_pct = 2.5
    atr_abs = 0
    if n_use >= 20:
        trs = []
        for i in range(1, len(cl120)):
            prev_c = cl120[i - 1]
            tr = max(hi120[i] - lo120[i],
                     abs(hi120[i] - prev_c),
                     abs(lo120[i] - prev_c))
            trs.append(tr)
        if trs:
            tr14 = trs[-14:] if len(trs) >= 14 else trs
            atr14 = sum(tr14) / len(tr14)
            atr_abs = atr14
            atr_pct = atr14 / cl120[-1] * 100 if cl120[-1] > 0 else 2.5
    if atr_pct < 0.3:
        atr_pct = 0.3
    if atr_abs <= 0:
        atr_abs = c * atr_pct / 100
    _vol_level_gx = ("稳波动(ATR<4%)" if atr_pct < 4 else "高波动(ATR≥4%)")
    rets = [(cl120[i] / cl120[i - 1] - 1) * 100 for i in range(1, len(cl120)) if cl120[i - 1] > 0]
    _neg_rets_gx = [r for r in rets if r < 0]
    q25_ret = abs(_quantile(_neg_rets_gx, 0.25)) if _neg_rets_gx else atr_pct * 0.8
    q50_ret = abs(_quantile(_neg_rets_gx, 0.50)) if _neg_rets_gx else atr_pct * 1.3
    q75_ret = abs(_quantile(_neg_rets_gx, 0.75)) if _neg_rets_gx else atr_pct * 2.2
    lo_min = min(lo120)
    lo_q20 = _quantile(lo120, 0.20)
    if not math.isnan(ma20[-1]):
        _support_level_gx = round((lo_min + lo_q20 + min(ma20[-1], c)) / 3, 2)
    else:
        _support_level_gx = round((lo_min + lo_q20 * 2) / 3, 2)
    ma20_dip_avg = atr_pct * 1.2
    _dip_depths_gx = []
    if n_use >= 40:
        ma20_s = ma20[c_start:]
        for i in range(len(cl120)):
            if i >= len(ma20_s) or math.isnan(ma20_s[i]) or ma20_s[i] <= 0:
                continue
            if lo120[i] < ma20_s[i]:
                d = (1 - lo120[i] / ma20_s[i]) * 100
                if 0 < d < 25:
                    _dip_depths_gx.append(d)
        if len(_dip_depths_gx) >= 3:
            ma20_dip_avg = sum(_dip_depths_gx) / len(_dip_depths_gx)
    _rebound_count_gx = 0
    _total_touch_gx = 0
    if n_use >= 40 and not math.isnan(ma20[-1]):
        ma20_s = ma20[c_start:]
        state = 0
        anchor = 0
        for i in range(len(cl120)):
            if i >= len(ma20_s) or math.isnan(ma20_s[i]) or ma20_s[i] <= 0:
                continue
            dist = (cl120[i] / ma20_s[i] - 1) * 100
            # 20260904 用户改: 触及 MA20 阈值 ±3% (旧 ±2.5%), 反弹幅度 ≥5% (旧 ≥3%)
            if state == 0 and -3.0 <= dist <= 3.0:
                state = 1
                _total_touch_gx += 1
                anchor = cl120[i]
            elif state == 1 and anchor > 0:
                if (cl120[i] / anchor - 1) * 100 >= 5:
                    state = 2
                    _rebound_count_gx += 1
                elif dist > 4 or dist < -5:
                    state = 0
    # ============ 近半年股性特征提取 END (价格与desc在return前组装) ============

    # --- MACD & KDJ ---
    dif, dea, macd_hist = calc_macd(closes)
    _k, _d_kdj, _j = calc_kdj(highs, lows, closes)
    J_t = _j[-1] if not math.isnan(_j[-1]) else 50.0
    K_t = _k[-1] if not math.isnan(_k[-1]) else 50.0
    macd_dif = dif[-1] if not math.isnan(dif[-1]) else 0
    macd_dea = dea[-1] if not math.isnan(dea[-1]) else 0
    macd_hist_t = macd_hist[-1] if not math.isnan(macd_hist[-1]) else 0
    # MACD金叉(近3日内DIF上穿DEA)
    macd_golden = False
    for i in range(max(1, len(closes) - 3), len(closes)):
        p = i - 1
        if p < 0 or any(math.isnan(x) for x in (dif[i], dea[i], dif[p], dea[p])):
            continue
        if dif[p] <= dea[p] and dif[i] > dea[i]:
            macd_golden = True
    # MACD零轴上方
    macd_above_zero = macd_dif > 0 and macd_dea > 0
    # KDJ低位金叉(J<30且J上行)
    kdj_low = J_t < 30
    kdj_golden_low = kdj_low and len(_j) >= 2 and not math.isnan(_j[-2]) and J_t > _j[-2]

    # --- 1. 趋势方向: MA20连续3日方向 ---
    ma20_up3 = all(not math.isnan(ma20[i]) and ma20[i] > ma20[i-1] for i in range(-3, 0))
    ma20_dn3 = all(not math.isnan(ma20[i]) and ma20[i] < ma20[i-1] for i in range(-3, 0))
    if ma20_up3:
        trend = "多头趋势"
    elif ma20_dn3:
        trend = "空头趋势"
    else:
        trend = "震荡"
    # 走平判定: 近6日MA20波动<0.5%
    ma20_flat = False
    if len(ma20) >= 7 and not any(math.isnan(ma20[i]) for i in range(-7, 0)):
        ma20_flat = all(abs(ma20[i] - ma20[i-1]) / ma20[i-1] < 0.005 for i in range(-6, 0))

    # --- 2. 均线排列 ---
    has_ma60 = not math.isnan(ma60[-1])
    if not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) and not math.isnan(ma20[-1]):
        if has_ma60 and ma5[-1] > ma10[-1] > ma20[-1] > ma60[-1]:
            alignment = "多头排列"
        elif ma5[-1] < ma10[-1] < ma20[-1] and has_ma60 and ma10[-1] < ma60[-1]:
            alignment = "空头排列"
        elif ma5[-1] > ma10[-1] > ma20[-1]:
            alignment = "短多头"
        elif ma5[-1] < ma10[-1] < ma20[-1]:
            alignment = "短空头"
        else:
            alignment = "交叉纠缠"
    else:
        alignment = "数据不足"

    # 5/10/20日线空头排列 (不要求MA60, 用于卖点3)
    bear_align_51020 = (not math.isnan(ma5[-1])) and (not math.isnan(ma10[-1])) and (not math.isnan(ma20[-1])) and ma5[-1] < ma10[-1] < ma20[-1]

    # --- 均线粘合判定 (5/10/20/60线收敛) ---
    ma_converge = False
    if not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) and not math.isnan(ma20[-1]) and not math.isnan(ma60[-1]):
        max_ma = max(ma5[-1], ma10[-1], ma20[-1], ma60[-1])
        min_ma = min(ma5[-1], ma10[-1], ma20[-1], ma60[-1])
        ma_converge = (max_ma - min_ma) / min_ma * 100 < 3.0  # 4线振幅<3%

    # --- 3. 乖离率 ---
    # 卖点5 过热止盈: 乖离基准 MA20→MA10 (更灵敏, 更早识别过热)
    bias = (c - ma10[-1]) / ma10[-1] * 100 if not math.isnan(ma10[-1]) and ma10[-1] > 0 else 0

    # --- 4. 量能 ---
    vol_avg5 = sum(vols[-6:-1]) / 5 if len(vols) >= 6 else 0
    vol_avg10 = sum(vols[-11:-1]) / 10 if len(vols) >= 11 else 0
    vol_avg20 = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else 0
    today_vol = vols[-1]
    vol_shrink = vol_avg5 > 0 and today_vol < vol_avg5 * 0.8
    vol_expand = vol_avg5 > 0 and today_vol > vol_avg5 * 1.5
    vol_ratio = today_vol / vol_avg5 if vol_avg5 > 0 else 1.0

    # --- 5. 距均线位置 ---
    dist_ma20_pct = abs(c / ma20[-1] - 1) * 100 if not math.isnan(ma20[-1]) and ma20[-1] > 0 else 999
    near_ma20 = dist_ma20_pct <= 2.0  # ±2%区间
    above_ma20 = c > ma20[-1]
    below_ma20 = c < ma20[-1]

    # --- 6. K线形态 ---
    # 止跌K线: 下影线长 or 阳包阴
    body = abs(c - opens[-1])
    lower_shadow = min(opens[-1], c) - lows[-1] if len(lows) >= 1 else 0
    upper_shadow = highs[-1] - max(opens[-1], c)
    is_yang = c > opens[-1]
    is_stop_drop = (lower_shadow > body * 0.5 and lower_shadow > 0) or \
                   (is_yang and len(closes) >= 2 and c > opens[-1] and closes[-2] < opens[-2] and c > closes[-2])
    # 滞涨K线: 上影线长 + 涨幅小
    chg_today = (c / closes[-2] - 1) * 100 if len(closes) >= 2 else 0
    is_stagnant = upper_shadow > body * 0.5 and abs(chg_today) < 2

    # --- 7. 金叉/死叉检测 -------------------------------
    #   20260902 用户修正:
    #     1) 交叉窗口从 5 日 → 2 日 (只用今昨两天, 避免V反后把4天前的旧死叉当卖出信号)
    #     2) 「卖点4 死叉」需要 MA5和MA10 都下穿MA20 (双重确认, 单一均线下穿不算死叉, 震荡噪音过滤)
    #     3) 波段票 降低均线比重: 即使判出死叉, 只要收盘站上MA20且≥1% 就不算死叉, 波段票更看前低/支撑/量能
    #        (用户原话: 波段票买点在前期低点附近或者踩均线附近买, 均线比重不要太大)
    #   参数: 交叉窗口 win=2 (今昨);  双重确认 double_req=True;  波段票豁免 swing_ma_immune=True
    WIN_CROSS = 2            # 只用最近2根K线 (index∈[-2,-1])
    DOUBLE_CONFIRM = True    # 死叉/金叉: MA5和MA10 必须同时同向交叉
    SWING_MA_IMMUNE = True   # 波段票: 收盘站上MA20+≥1% 时 → 直接免疫死叉/金叉降级

    golden_cross = False
    death_cross = False
    cross_win_start = max(1, len(closes) - WIN_CROSS)
    for i in range(cross_win_start, len(closes)):
        p = i - 1
        if p < 0: continue
        def _crs(arr_short, arr_long, i, p, sense='golden'):
            # sense: golden (短从下穿上长) / death (短从上穿下长)
            if any(math.isnan(x) for x in (arr_short[i], arr_long[i], arr_short[p], arr_long[p])):
                return False
            if sense == 'golden': return arr_short[p] <= arr_long[p] and arr_short[i] > arr_long[i]
            else:                 return arr_short[p] >= arr_long[p] and arr_short[i] < arr_long[i]
        ma5_cross_g = _crs(ma5, ma20, i, p, 'golden')
        ma5_cross_d = _crs(ma5, ma20, i, p, 'death')
        ma10_cross_g = _crs(ma10, ma20, i, p, 'golden')
        ma10_cross_d = _crs(ma10, ma20, i, p, 'death')
        if DOUBLE_CONFIRM:
            # 双重确认: MA5和MA10 同时同向穿过MA20, 或至少其一另一方向也不反向(至少同方向倾斜)
            #   金叉: MA5金叉 + (MA10金叉 or MA10今日>MA10昨日且MA10今日距离MA20<1.5%)
            if (ma5_cross_g and ma10_cross_g):
                golden_cross = True
            elif ma5_cross_g or ma10_cross_g:
                # 单一金叉: 需至少收盘站上MA20, 否则不算(过滤震荡假金叉)
                if not math.isnan(ma20[i]) and closes[i] > ma20[i]:
                    golden_cross = True
            if (ma5_cross_d and ma10_cross_d):
                death_cross = True
            elif ma5_cross_d or ma10_cross_d:
                # 单一死叉: 需收盘已跌破MA20, 否则不算(比如风语筑这种V反后还没上穿的震荡滞后)
                if not math.isnan(ma20[i]) and closes[i] < ma20[i] * 0.995:
                    death_cross = True
        else:
            if ma5_cross_g or ma10_cross_g: golden_cross = True
            if ma5_cross_d or ma10_cross_d: death_cross = True

    # → 波段票均线免疫: 先查 trend_score 和 swing_score 后面才有, 先存flag, return前再覆盖
    #   先记当前死叉来源窗口位置, 后面组装 style 后如果是波段票且站上MA20, 再覆盖death_cross=False

    # 死叉(MA5下穿MA10): 短期均线下穿MA10 (用于卖点4)
    death_cross_ma10 = False
    if len(closes) >= 2 and not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) \
       and not math.isnan(ma5[-2]) and not math.isnan(ma10[-2]):
        if ma5[-2] >= ma10[-2] and ma5[-1] < ma10[-1]:
            death_cross_ma10 = True

    # 死叉(MA5下穿MA20): MA5下穿MA20 (用于卖点6, 趋势票清仓)
    death_cross_ma20 = False
    if len(closes) >= 2 and not math.isnan(ma5[-1]) and not math.isnan(ma20[-1]) \
       and not math.isnan(ma5[-2]) and not math.isnan(ma20[-2]):
        if ma5[-2] >= ma20[-2] and ma5[-1] < ma20[-1]:
            death_cross_ma20 = True

    # --- 8. 有效突破/跌破判定 ---
    # 突破: 前日在MA20下方, 昨日收盘站上, 今日不收回下方
    breakout_above = False
    if len(closes) >= 3 and not math.isnan(ma20[-1]):
        was_below = closes[-3] < ma20[-3] if not math.isnan(ma20[-3]) else False
        crossed_up = closes[-2] > ma20[-2] if not math.isnan(ma20[-2]) else False
        stays_up = c > ma20[-1]
        breakout_above = was_below and crossed_up and stays_up and chg_today > 2
    # 跌破: 前日在MA20上方, 昨日收盘跌破, 今日不收回上方
    breakdown_below = False
    if len(closes) >= 3 and not math.isnan(ma20[-1]):
        was_above = closes[-3] > ma20[-3] if not math.isnan(ma20[-3]) else False
        crossed_dn = closes[-2] < ma20[-2] if not math.isnan(ma20[-2]) else False
        stays_dn = c < ma20[-1]
        breakdown_below = was_above and crossed_dn and stays_dn and chg_today < -2
    # 跌破MA10: 前日在MA10上方, 昨日收盘跌破, 今日不收回上方, 跌幅>2% (用于卖点2)
    breakdown_below_ma10 = False
    if len(closes) >= 3 and not math.isnan(ma10[-1]):
        was_above_ma10 = closes[-3] > ma10[-3] if not math.isnan(ma10[-3]) else False
        crossed_dn_ma10 = closes[-2] < ma10[-2] if not math.isnan(ma10[-2]) else False
        stays_dn_ma10 = c < ma10[-1]
        breakdown_below_ma10 = was_above_ma10 and crossed_dn_ma10 and stays_dn_ma10 and chg_today < -2

    # --- 9. 过热判定 ---
    overbought = bias > 10 and (is_stagnant or vol_expand)

    # --- 10. 趋势票强制判定: 连续5天收盘>MA5 且 5天涨幅>10% → 趋势票 ---
    force_trend = False
    if len(closes) >= 6 and len(ma5) >= 5:
        ma5_last5 = ma5[-5:]
        closes_last5 = closes[-5:]
        if all(not math.isnan(m) for m in ma5_last5) and \
           all(cl > m for cl, m in zip(closes_last5, ma5_last5)):
            gain_5d = (closes[-1] / closes[-6] - 1) * 100
            if gain_5d > 10:
                force_trend = True

    # 趋势票候选 (用于卖点前置过滤, style_type在后面才确认, return前再次校验)
    _is_trend_candidate = force_trend or (trend == "多头趋势" and alignment == "多头排列")

    # 趋势停滞: 连续4天累计涨幅≤3% (用于卖点7, 仅趋势票)
    # 20260903 用户追加: 低量涨停不算滞涨, 尾盘涨停当天也不卖出
    trend_stagnant = False
    if len(closes) >= 5:
        r4 = (c / closes[-5] - 1) * 100
        if r4 <= 3:
            # 判定今日是否涨停 (用 _is_limit_up 辅助函数)
            _today_limit_up = False
            if len(closes) >= 2 and closes[-2] > 0:
                _chg_pct = (c / closes[-2] - 1) * 100
                _code = (bars[-1].get("symbol", "") or "") if isinstance(bars[-1], dict) else ""
                if _code.startswith(("sz30", "sh688", "sz301")):
                    _today_limit_up = _chg_pct >= 19.5
                else:
                    _today_limit_up = _chg_pct >= 9.8
            # 低量涨停: 今日涨停 + 缩量(vol_ratio < 0.8) → 不算滞涨
            _low_vol_limit = _today_limit_up and vol_ratio < 0.8
            # 尾盘涨停: 今日涨停 → 当天不卖出
            if _low_vol_limit or _today_limit_up:
                trend_stagnant = False
            else:
                trend_stagnant = True

    # ===== 买卖点判定 =====
    signal = "wait"
    signal_type = ""
    entry_price = None
    stop_loss = None
    target_price = None
    reason_parts = []
    actions = []

    # --- 买点1: 回踩不破 (多头趋势中缩量回踩MA20±2% + 止跌企稳) ---
    if trend in ("多头趋势",) and near_ma20 and above_ma20:
        if vol_shrink or is_stop_drop:
            signal = "buy"
            signal_type = "买点1: 回踩不破"
            entry_price = round(c, 2)
            stop_loss = round(ma20[-1] * 0.98, 2)
            # 目标: 前高 or 乖离10%位置
            hi20 = max(highs[-20:]) if len(highs) >= 20 else max(highs)
            target_price = round(max(hi20, c * 1.08), 2)
            reason_parts.append("多头趋势中缩量回踩MA20±2%区域")
            if is_stop_drop:
                reason_parts.append("出现止跌K线信号")
            if vol_shrink:
                reason_parts.append("回调缩量(卖盘减弱)")
            actions.append("企稳放量当天尾盘或次日高开不破均线时买入")

    # --- 买点2: 上穿突破 (放量突破MA20, 量能≥1.5倍) ---
    # [已禁用] 用户要求注释掉买点2
    # if signal == "wait" and breakout_above and vol_expand:
    #     signal = "buy"
    #     signal_type = "买点2: 上穿突破"
    #     entry_price = round(c, 2)
    #     stop_loss = round(ma20[-1] * 0.97, 2)
    #     hi20 = max(highs[-20:]) if len(highs) >= 20 else c * 1.1
    #     target_price = round(max(hi20, c * 1.10), 2)
    #     reason_parts.append(f"放量突破MA20(量比{vol_ratio:.1f}倍)")
    #     reason_parts.append("突破幅度>2%且站稳")
    #     actions.append("激进者突破当日入场; 稳健者等回踩MA20不破再进")

    # --- 买点3: 金叉买入 (MA5/MA10上穿MA20, 回踩金叉位不破) ---
    if signal == "wait" and golden_cross and trend != "空头趋势":
        # 金叉后回踩MA20不破
        if near_ma20 and above_ma20:
            signal = "buy"
            signal_type = "买点3: 金叉买入"
            entry_price = round(c, 2)
            stop_loss = round(ma20[-1] * 0.98, 2)
            target_price = round(c * 1.10, 2)
            reason_parts.append("短期均线上穿MA20形成金叉")
            reason_parts.append("回踩金叉位不破, 二次确认")
            actions.append("金叉形成后回踩交叉位不破时买入")

    # --- 买点4: 多头排列中回踩MA5加仓 (仅趋势票触发) ---
    # 20260903 用户修正: 只在趋势票触发; "收盘仍在MA20上方"改为"仍在MA5上方"(逻辑+文案)
    #   注: style_type 在后面才组装, 此处先用 alignment==多头排列 + trend==多头趋势 作为趋势票代理,
    #       return前会再次校验 style_type 含"趋势", 非趋势票降级为wait
    if signal == "wait" and alignment == "多头排列" and trend == "多头趋势":
        near_ma5 = abs(c / ma5[-1] - 1) * 100 <= 2 if not math.isnan(ma5[-1]) and ma5[-1] > 0 else False
        if near_ma5 and not math.isnan(ma5[-1]) and c > ma5[-1]:
            signal = "buy"
            signal_type = "买点4: 多头排列加仓"
            entry_price = round(c, 2)
            stop_loss = round(ma20[-1] * 0.98, 2)
            target_price = round(c * 1.10, 2)
            reason_parts.append("多头排列确立, 回踩5日线企稳, 收盘仍在MA5上方")
            actions.append("回调加仓, 非开新仓首选(仅趋势票)")

    # --- 卖点1: 反弹不过 (空头趋势中反弹至MA20±2%受阻) ---
    if signal == "wait" and trend in ("空头趋势",) and near_ma20 and below_ma20:
        if is_stagnant or (not is_yang and chg_today < 1):
            signal = "sell"
            signal_type = "卖点1: 反弹不过"
            entry_price = round(c, 2)
            stop_loss = round(ma20[-1] * 1.02, 2)  # 反弹止损=MA20上方
            target_price = round(min(lows[-20:]) * 0.97, 2) if len(lows) >= 20 else round(c * 0.92, 2)
            reason_parts.append("空头趋势中反弹至MA20±2%受阻")
            reason_parts.append("出现滞涨K线, 反弹乏力")
            actions.append("反弹到位即减仓/清仓, 不等跌破")

    # --- 卖点2: 跌破MA10 (仅趋势票触发, 收盘价有效跌破MA10, 跌幅>2%) ---
    # 20260903 用户修正: 只在趋势票触发; MA20→MA10
    if signal == "wait" and breakdown_below_ma10:
        signal = "sell"
        signal_type = "卖点2: 跌破MA10"
        entry_price = round(c, 2)
        stop_loss = round(ma10[-1] * 1.02, 2) if not math.isnan(ma10[-1]) else round(c * 1.03, 2)
        target_price = round(c * 0.92, 2)
        reason_parts.append("收盘价有效跌破MA10, 跌幅>2%")
        reason_parts.append("3日内若收不回MA10上方→清仓")
        actions.append("当日减仓; 3日收不回→清仓止损(仅趋势票)")

    # --- 卖点3: 空头排列 (5/10/20日线空头排列确立) ---
    # 20260903 用户修正: 空头排列定义改为5/10/20日线(不要求MA60)
    if signal == "wait" and bear_align_51020:
        signal = "sell"
        signal_type = "卖点3: 空头排列"
        entry_price = round(c, 2)
        stop_loss = round(ma10[-1] * 1.02, 2) if not math.isnan(ma10[-1]) else round(c * 1.03, 2)
        target_price = round(c * 0.90, 2)
        reason_parts.append("5/10/20日线空头排列确立")
        reason_parts.append("每次反弹至均线压制位都是卖点")
        actions.append("不抄底, 反弹至均线压制位减仓")

    # --- 卖点4: 死叉 (短期均线下穿MA10形成死叉) ---
    # 20260903 用户修正: 死叉定义改为短期均线下穿MA10
    # 20260903 用户追加: 趋势票 MA5下叉MA10 → 减半仓(当天收盘价); 非趋势票 全清
    if signal == "wait" and death_cross_ma10:
        signal = "sell"
        signal_type = "卖点4: 死叉"
        entry_price = round(c, 2)
        stop_loss = round(ma10[-1] * 1.02, 2) if not math.isnan(ma10[-1]) else round(c * 1.03, 2)
        target_price = round(c * 0.92, 2)
        reason_parts.append("短期均线下穿MA10形成死叉")
        if _is_trend_candidate:
            reason_parts.append("趋势票: MA5下叉MA10 → 减半仓(当天收盘价)")
            actions.append("趋势票减半仓(当天收盘价); 反弹不过死叉位→再减")
        else:
            actions.append("止盈止损离场; 反弹不过死叉位→最后离场点")

    # --- 卖点6: MA5下穿MA20 (趋势票清仓, 当天收盘价) ---
    # 20260903 用户追加: 趋势票 MA5下叉MA20 → 清仓(当天收盘价)
    if signal == "wait" and death_cross_ma20 and _is_trend_candidate:
        signal = "sell"
        signal_type = "卖点6: MA5下穿MA20"
        entry_price = round(c, 2)
        stop_loss = round(ma20[-1] * 1.02, 2) if not math.isnan(ma20[-1]) else round(c * 1.03, 2)
        target_price = round(c * 0.90, 2)
        reason_parts.append("MA5下穿MA20, 趋势票清仓(当天收盘价)")
        actions.append("趋势票清仓(当天收盘价)")

    # --- 卖点7: 趋势停滞 (趋势票连续4天累计涨幅≤3%) ---
    # 20260903 用户追加: 趋势票连续4天涨幅不超过3% → 卖出
    # 20260903 用户修正: 低量涨停不算滞涨, 尾盘涨停当天也不卖出
    if signal == "wait" and trend_stagnant and _is_trend_candidate:
        signal = "sell"
        signal_type = "卖点7: 趋势停滞"
        entry_price = round(c, 2)
        stop_loss = round(ma5[-1] * 1.02, 2) if not math.isnan(ma5[-1]) else round(c * 1.03, 2)
        target_price = round(c * 0.95, 2)
        r4_val = (c / closes[-5] - 1) * 100 if len(closes) >= 5 else 0
        reason_parts.append(f"趋势票连续4天累计涨幅仅{r4_val:.1f}%≤3%, 趋势停滞")
        reason_parts.append("排除: 低量涨停/尾盘涨停不算滞涨, 当天不卖")
        actions.append("趋势停滞, 减仓或离场")

    # --- 过热止盈 (叠加在买入信号上的警告) ---
    if overbought and signal != "sell":
        if signal == "buy":
            signal_type += " ⚠过热"
            reason_parts.append(f"乖离率{bias:.1f}%>10%, 已过热")
            reason_parts.append("出现放量滞涨/长上影, 建议分批止盈")
            actions.append("至少减掉1/3仓位")
        elif signal == "wait":
            signal = "sell"
            signal_type = "卖点: 过热止盈"
            entry_price = round(c, 2)                              # 建议止盈价(当前价)
            stop_loss = round(c * 1.03, 2)                         # 取消位: 再涨3%则取消止盈(可能进入加速段)
            target_price = round(ma10[-1], 2) if not math.isnan(ma10[-1]) else round(c * 0.90, 2)  # 回踩目标 (MA10)
            reason_parts.append(f"乖离率{bias:.1f}%>10%")
            reason_parts.append("出现放量滞涨/长上影线")
            actions.append("分批止盈, 至少减掉1/3")

    # --- 量能过滤: 缩量突破/反弹 = 假信号 (买点2已禁用, 此分支不再触发) ---
    # if signal == "buy" and signal_type.startswith("买点2") and not vol_expand:
    #     signal = "wait"
    #     reason_parts.append("量能不足, 突破可信度低, 观望")
    #     actions = ["量能不配合, 暂观望"]

    # --- 震荡市: 均线走平时停用 ---
    if signal == "wait" and ma20_flat:
        reason_parts.append("MA20走平, 震荡市该策略停用")
        actions.append("空仓等待趋势明朗")

    # --- 默认wait给原因 ---
    if signal == "wait" and not reason_parts:
        if trend == "多头趋势" and above_ma20 and dist_ma20_pct > 5:
            reason_parts.append(f"多头趋势但股价偏离MA20达{dist_ma20_pct:.1f}%, 等回踩")
            actions.append("等待回踩MA20±2%区域再考虑")
        elif trend == "空头趋势":
            reason_parts.append("空头趋势, 不做多")
            actions.append("空仓观望或等待空头排列卖点")
        elif above_ma20:
            reason_parts.append("股价站上MA20但趋势不明")
            actions.append("等待趋势确认或信号出现")
        else:
            reason_parts.append("股价在MA20下方, 无买入信号")
            actions.append("观望")

    # 量能状态文字
    if vol_shrink:
        vol_status = "缩量"
    elif vol_expand:
        vol_status = "放量"
    else:
        vol_status = "正常"

    # ===== 指标共振检测 =====
    resonance = []
    resonance_count = 0
    # 量能共振: 缩量回踩 + 企稳放量
    if signal == "buy" and vol_shrink:
        resonance.append("量能: 回踩缩量企稳, 资金认可支撑")
        resonance_count += 1
    # [已禁用] 买点2量能共振
    # if signal == "buy" and vol_expand and signal_type.startswith("买点2"):
    #     resonance.append("量能: 放量突破确认, 买盘强劲")
    #     resonance_count += 1
    # MACD共振: 零轴上方金叉 or 零轴上方运行
    if macd_above_zero:
        if macd_golden:
            resonance.append("MACD: 零轴上方金叉, 趋势与动能共振")
            resonance_count += 1
        elif signal == "buy":
            resonance.append("MACD: 零轴上方运行, 多头动能延续")
            resonance_count += 1
    elif macd_golden and signal == "buy":
        resonance.append("MACD: 零轴下方金叉, 动能开始转强")
        resonance_count += 1
    # KDJ共振: 低位金叉向上
    if kdj_golden_low and signal == "buy":
        resonance.append("KDJ: 低位金叉向上, 超跌反弹+支撑有效")
        resonance_count += 1
    elif J_t < 50 and not math.isnan(_j[-2]) and J_t > _j[-2] and signal == "buy":
        resonance.append("KDJ: 中低位拐头向上, 动能增强")
        resonance_count += 1

    # ===== 误区提示 =====
    warnings = []
    # ① 震荡市金叉信号不可靠
    if trend == "震荡" and golden_cross:
        warnings.append("① 震荡市金叉信号反复, 容易被反复打脸, 需趋势确认后再操作")
    # ② 均线粘合时方向未明
    if ma_converge:
        warnings.append("④ 均线粘合, 方向未明, 不宜重仓出击, 等待发散方向")
    # ③ 跌破均线勿急于止损(容忍假摔)
    if signal == "sell" and signal_type.startswith("卖点2"):
        warnings.append("⑤ 跌破均线时容忍1-2天假摔, 第3天收不回再止损, 避免被洗盘")
    # ④ 空头排列中不抄底
    if alignment == "空头排列" and signal == "wait":
        warnings.append("空头排列中不抄底, 每次反弹至均线压制位都是卖点")
    # ⑤ 乖离过大追高风险
    if bias > 8 and signal != "sell":
        warnings.append(f"乖离率{bias:.1f}%, 距MA20过远, 追高风险增大")

    # ===== 交易箴言 =====
    motto = "趋势不变仓位不乱, 均线不破格局不散, 控制风险活得久"
    if signal == "buy":
        motto = "买点要'缩量来、放量走'; 买前先写好入场价、止损价、目标价"
    elif signal == "sell":
        motto = "卖点与买点镜像对称; 严格执行止损, 保护本金"
    elif trend == "震荡":
        motto = "均线走平时该策略停用; 震荡市少做或不做"

    # 信号侧: buy=做多入场, sell=多单离场(A股不可做空), wait=观望
    side = "buy" if signal == "buy" else ("sell" if signal == "sell" else "wait")

    # ============ 持仓视角: 已持有情况下的止盈/止损/减仓建议 ============
    # 止盈分批位 (以MA20和近20日高点为锚, 保守/中性/激进三档)
    hi20 = max(highs[-20:]) if len(highs) >= 20 else c
    hold_tp_conservative = round(max(ma20[-1], c * 1.03), 2) if not math.isnan(ma20[-1]) else round(c * 1.03, 2)
    hold_tp_neutral = round(max(hi20 * 0.97, c * 1.05), 2)
    hold_tp_aggressive = round(max(hi20, c * 1.08), 2)
    # 持仓止损 (保护线: 以MA20或近期低点为核心, 分动态/极端两档)
    if not math.isnan(ma20[-1]):
        hold_sl_dynamic = round(ma20[-1] * 0.98, 2)   # MA20跌破2% -> 触发减仓/止损
    else:
        hold_sl_dynamic = round(c * 0.97, 2)
    lo20 = min(lows[-20:]) if len(lows) >= 20 else c
    hold_sl_extreme = round(lo20 * 0.98, 2)         # 20日低点破位2% -> 清仓线

    # 新增两个关键中间量, 识别「假死叉」和「过热乖离」, 避免把刚V反/超强势误判成离场
    ma20_today = ma20[-1] if (len(ma20) > 0 and not math.isnan(ma20[-1])) else 0.0
    bias_to_ma20 = ((c / ma20_today) - 1) * 100 if ma20_today > 0 else 0.0   # 乖离率% (正=高于, 负=低于) - 用于假死叉/V反/均线相关判断
    # 卖点5 过热止盈基准 MA5 (20260903 用户修正: 基准从MA10改为MA5)
    ma10_today = ma10[-1] if (len(ma10) > 0 and not math.isnan(ma10[-1])) else 0.0
    bias_to_ma10 = ((c / ma10_today) - 1) * 100 if ma10_today > 0 else 0.0
    ma5_today = ma5[-1] if (len(ma5) > 0 and not math.isnan(ma5[-1])) else 0.0
    bias_to_ma5 = ((c / ma5_today) - 1) * 100 if ma5_today > 0 else 0.0
    # 近5日涨幅 (判断是否刚V型大阳反转)
    if len(closes) >= 6:
        r5 = ((c / closes[-6]) - 1) * 100
    elif len(closes) >= 2:
        r5 = ((c / closes[0]) - 1) * 100
    else:
        r5 = 0.0
    is_super_strong = (bias_to_ma20 >= 8) and (r5 >= 5)          # 高于MA20≥8% 且 5日累计≥5% = 强势
    is_false_death_cross = (signal == "sell") and (signal_type and "死叉" in signal_type) and (trend == "多头趋势") and (c > ma20_today) and (r5 >= 0)  # 死叉 但价在MA20上+趋势多+没跌 = 均线滞后的假死叉
    is_overheat = (bias_to_ma5 >= 10) and (r5 >= 20)             # 乖离≥10%(MA5基准) 且 5日累计≥20% = 过热(卖点5)

    # 持仓动作建议 (新增: 假死叉→持有观察; 过热→减1/3; 只有真的向下破位才离场)
    if signal == "sell" and alignment == "空头排列":
        hold_action = "清仓离场"
        hold_action_tip = "空头排列+卖出信号, 每一次反弹都是离场机会, 切不可逆势加仓"
    elif signal == "sell" and trend == "空头趋势" and not is_super_strong:
        hold_action = "逐步减仓, 跌破取消位清仓"
        hold_action_tip = "已处于空头趋势中, 建议减仓至少2/3, 剩余仓位严格用取消位做最后保护"
    elif alignment == "交叉纠缠" and trend != "多头趋势" and not is_super_strong:
        hold_action = "持有观察, 方向不明控制仓位"
        hold_action_tip = "均线缠绕方向未明, 不建议加仓, 仓位重的应减至半仓以下"
    elif signal == "buy":
        hold_action = "持股待涨, 按分批止盈位落袋"
        hold_action_tip = "当前处于买入信号区间, 可继续持有, 到止盈位分批减仓锁定利润"
    elif is_overheat:
        # 新增分支: 过热止盈(卖点5) → 减1/3 不是全走
        hold_action = "乖离过大, 先减至少1/3 (卖点5 过热止盈)"
        hold_action_tip = f"现价高于MA5已达 +{bias_to_ma5:.1f}%, 近5日累计 +{r5:.1f}%, 短期获利盘太丰厚; 但多头趋势不坏, 先止盈1/3锁定利润, 剩余用MA10({ma10_today:.2f})做移动止盈拿趋势, 若再涨3%才会进入加速段."
    elif is_false_death_cross or is_super_strong:
        # 新增分支: 假死叉 / V型刚反转走强 → 持有观察, 不是离场信号
        extra = " (假死叉: 均线滞后, 实际股价已站回MA20上方)" if is_false_death_cross else f" (V反: 近5日 +{r5:.1f}%, 高于MA20 +{bias_to_ma20:.1f}%)"
        hold_action = f"强势持有观察{extra}"
        hold_action_tip = f"短期进入强势区间, 不是真的向下破位, 不建议急着卖; 到上方止盈位(前高≈{hi20:.2f}附近或风格止盈)再分批落袋, 严格止损仍用MA20×0.98={round(ma20_today*0.98,2) if ma20_today>0 else '—'}做最后防线."
    elif signal == "wait" and trend == "多头趋势":
        hold_action = "继续持有, 关注止盈位"
        hold_action_tip = "多头趋势未破, 但无新买入信号, 到止盈位分批锁定利润"
    else:
        hold_action = "择机离场"
        hold_action_tip = "趋势不明或偏弱, 不建议继续持有, 反弹至压力位分批减仓"

    # suggest 与 action 语义对齐 (之前 suggest=持有 但 tip 写不建议持有 → 彻底修)
    if "清仓" in hold_action:
        hold_suggest = "清仓"
    elif "减" in hold_action or "离场" in hold_action:
        hold_suggest = "减仓"
    else:
        hold_suggest = "持有"

    # ============ 持仓视角 hold_view 组装 ============
    hold_view = {
        "tp_conservative": hold_tp_conservative,
        "tp_neutral": hold_tp_neutral,
        "tp_aggressive": hold_tp_aggressive,
        "sl_dynamic": hold_sl_dynamic,
        "sl_extreme": hold_sl_extreme,
        "action": hold_action,
        "action_tip": hold_action_tip,
        "suggest": hold_suggest,   # 与 action 语义严格一致
    }

    # ============ 多档买点: 基于近半年股性 (与 signal/trend 结合, 在return前最终组装) ============
    ma20_ref = ma20[-1] if (len(ma20) > 0 and not math.isnan(ma20[-1])) else c
    support_level = _support_level_gx
    vol_level = _vol_level_gx
    rebound_count = _rebound_count_gx
    total_touch = _total_touch_gx
    dip_depths = _dip_depths_gx

    # 买点1(激进): 信号确认时现价入场；否则 MA20 附近挂单
    if signal == "buy" and entry_price:
        t1_price = entry_price
    else:
        t1_price = round(max(ma20_ref * 0.99, c * 0.98), 2) if c > ma20_ref else round(c * 1.005, 2)

    # 买点2(稳健): MA20 - max(q50_ret, 1.5*ATR) 典型回调位 或 低于现价 q50_ret%
    t2_ma_based = round(ma20_ref * (1 - max(q50_ret, atr_pct * 1.5) / 100), 2)
    t2_retrace = round(c * (1 - q50_ret / 100), 2)
    t2_price = min(t2_ma_based, t2_retrace) if c > ma20_ref else t2_retrace

    # 买点3(保守): 强支撑位 / MA20 - 3ATR / 现价 90% 三者取有意义的最小值
    t3_ma_based = round(ma20_ref * (1 - max(q75_ret, atr_pct * 3.0) / 100), 2)
    t3_deep = round(c * (1 - q75_ret / 100), 2)
    t3_price_raw = min(t3_ma_based, t3_deep, support_level * 1.005) if c > ma20_ref else min(t3_deep, support_level * 1.005)
    t3_price = max(t3_price_raw, round(c * 0.90, 2))

    # 约束: 价格严格递减 t1 > t2 > t3 (价差≥1%)
    t1_price = max(round(t1_price, 2), round(t2_price * 1.01, 2), round(t3_price * 1.02, 2))
    t2_price = min(round(t1_price * 0.99, 2), max(round(t2_price, 2), round(t3_price * 1.01, 2)))
    t3_price = min(round(t2_price * 0.99, 2), round(t3_price, 2))

    # 统一止损: tier3 - 1*ATR 或 tier3*0.985 (取小), 再夹到 [tier3*96%, tier3*99%]
    sl_price_raw = round(t3_price - atr_abs, 2)
    sl_price = min(sl_price_raw, round(t3_price * 0.985, 2))
    sl_price = max(sl_price, round(t3_price * 0.96, 2))
    sl_price = min(sl_price, round(t3_price * 0.99, 2))
    t1_price, t2_price, t3_price, sl_price = round(t1_price, 2), round(t2_price, 2), round(t3_price, 2), round(sl_price, 2)

    # ===== 每档原因描述 =====
    if signal == "buy":
        t1_desc = f"当前{signal_type or '买入信号'}, 信号确认可直接入场; 近半年ATR={atr_pct:.1f}%({vol_level}), 跌破止损位一律离场"
    else:
        s1 = (1 - t1_price / c) * 100 if c > 0 else 0
        t1_desc = f"靠近MA20({ma20_ref:.2f})挂单位, 与现价价差≈{s1:.1f}%; 若之后出现买入信号可直接按此位入场, 否则建议等t2/t3"

    t2_spread_pct = (1 - t2_price / c) * 100 if c > 0 else 0
    if len(dip_depths) >= 3:
        t2_desc = f"典型回调位: 近半年负收益q50={q50_ret:.1f}%, 跌破MA20平均幅度={ma20_dip_avg:.1f}%; 此位企稳买入胜率较优, 与现价价差≈{t2_spread_pct:.1f}%"
    else:
        t2_desc = f"典型回调位: q50{q50_ret:.1f}% + ATR{atr_pct:.1f}% 组合锚定的稳健挂单, 与现价价差≈{t2_spread_pct:.1f}%"

    t3_spread_pct = (1 - t3_price / c) * 100 if c > 0 else 0
    t3_desc = f"保守挂单: 近半年强支撑≈{support_level:.2f}, 深幅回调q75={q75_ret:.1f}%; 赔率优先策略, 与现价价差≈{t3_spread_pct:.1f}%, 跌破止损严格执行"

    # ===== 股性摘要 =====
    parts = []
    warn_prefix = ""
    if signal == "sell":
        warn_prefix = "⚠当前【卖出信号 / 多单离场】，以下为计划挂单买点，条件未触发前切勿提前入场；空头趋势中宁错过勿抄底。"
    elif signal == "wait" and trend != "多头趋势":
        warn_prefix = "⚠当前无明确买入信号，以下为基于近半年股性测算的计划挂单买点，不代表建议立即买入。"
    parts.append(f"近半年波动：ATR14={atr_pct:.1f}%（{vol_level}）")
    if n_use >= 40 and not math.isnan(ma20[-1]):
        parts.append(f"MA20±2.5%区间共回踩{total_touch}次，其中反弹≥3%有{rebound_count}次")
    total_spread = (1 - t3_price / (t1_price if t1_price else c)) * 100
    parts.append(f"3档买点总价差（买1→买3）≈{total_spread:.1f}%")
    sl_dist = (1 - sl_price / t3_price) * 100
    parts.append(f"统一止损：{sl_price:.2f}（买3下方≈{sl_dist:.1f}%）")
    if n_use < 60:
        parts.append("K线不足半年，档位间距采用均线近似算法")
    reason_summary = warn_prefix + "；".join(parts)

    buy_tiers = {
        "tier1": {"price": t1_price, "label": "买点1(激进)", "desc": t1_desc},
        "tier2": {"price": t2_price, "label": "买点2(稳健)", "desc": t2_desc},
        "tier3": {"price": t3_price, "label": "买点3(保守)", "desc": t3_desc},
        "stop_loss": sl_price,
        "volatility": vol_level,
        "atr_pct": round(atr_pct, 2),
        "support_level": support_level,
        "reason_summary": reason_summary,
    }
    # ============ 多档买点 END ============

    # ============ 我的持仓风格 → 基于近半年股性 & 趋势判定, 匹配用户风格 ============
    # 风格规则: 趋势票 持仓1~2周 / 波段票 持仓2~5天 / 避坑票 不建议入场
    # -- 额外特征提取: MA20 4日斜率 / 近20日涨跌连贯性 / 近60日净涨跌 / 波段运行长度统计 --
    _style_extra = {}
    # MA20 4日斜率 (近期趋势强弱, 避免20日窗口过于滞后)
    ma20_slope_pct = 0.0
    ma20_20d_up_streak = 0
    if len(ma20) >= 5 and not any(math.isnan(ma20[i]) for i in range(-5, 0)):
        ma20_now = ma20[-1]
        ma20_4ago = ma20[-5]
        if ma20_4ago > 0:
            ma20_slope_pct = (ma20_now / ma20_4ago - 1) * 100  # 4日MA20累计涨跌%
        # MA20连续向上天数(近20日内的连续上升)
        s = 0
        for i in range(-20, 0):
            if ma20[i] > ma20[i - 1]:
                s += 1
                ma20_20d_up_streak = max(ma20_20d_up_streak, s)
            else:
                s = 0
    _style_extra["ma20_slope_4d_pct"] = round(ma20_slope_pct, 2)
    _style_extra["ma20_up_streak_20d_max"] = ma20_20d_up_streak

    # 近60日净涨跌 (与MA20斜率一起确认中期趋势强度)
    c60_ago = closes[-60] if len(closes) >= 60 else closes[0]
    chg60_pct = (c / c60_ago - 1) * 100 if c60_ago > 0 else 0

    # 近20日日K阳线/阴线比率 (连贯性)
    n20 = min(20, len(closes))
    c20_start = closes[-n20]
    up_days_20 = sum(1 for i in range(-n20, 0) if closes[i] >= opens[i])
    pos_days_20 = sum(1 for i in range(-n20 + 1, 0) if closes[i] > closes[i - 1])
    pos_ratio_20 = pos_days_20 / max(1, n20 - 1)  # 正收益日占比 (越高越单边)

    # 近20日单边波段平均长度 (连涨/连跌的平均天数)
    run_lens_pos = []
    run_lens_neg = []
    cur = 1
    prev_sig = 0
    for i in range(-min(60, len(closes)) + 1, 0):
        sig = 1 if closes[i] > closes[i - 1] else (-1 if closes[i] < closes[i - 1] else 0)
        if sig == 0:
            continue
        if sig == prev_sig:
            cur += 1
        else:
            if prev_sig == 1 and cur >= 2:
                run_lens_pos.append(cur)
            elif prev_sig == -1 and cur >= 2:
                run_lens_neg.append(cur)
            cur = 1
            prev_sig = sig
    if prev_sig == 1 and cur >= 2:
        run_lens_pos.append(cur)
    elif prev_sig == -1 and cur >= 2:
        run_lens_neg.append(cur)
    avg_run_pos = sum(run_lens_pos) / len(run_lens_pos) if run_lens_pos else 1
    avg_run_neg = sum(run_lens_neg) / len(run_lens_neg) if run_lens_neg else 1
    _style_extra["avg_up_run_days_60d"] = round(avg_run_pos, 1)
    _style_extra["avg_down_run_days_60d"] = round(avg_run_neg, 1)

    # ====== 打分: 趋势得分 / 波段得分 (0~100, 越大越像) ======
    trend_score = 50.0
    swing_score = 50.0

    # 1. 排列 + 趋势 (权重最高)
    if alignment == "多头排列" and trend == "多头趋势":
        trend_score += 30
        swing_score -= 10
    elif alignment == "空头排列":
        trend_score -= 15
        swing_score += 5
    elif alignment == "交叉纠缠":
        trend_score -= 15
        swing_score += 25
    if trend == "震荡":
        swing_score += 20
        trend_score -= 10
    elif trend == "多头趋势":
        trend_score += 15
    elif trend == "空头趋势":
        trend_score -= 5

    # 2. MA20 斜率 (趋势性核心)
    # 关键约束: 只有「多头排列」时 MA20 斜率向上才是真趋势信号!
    # 交叉纠缠/空头排列时, MA20 上移只是均线滞后 (比如横盘震荡 MA20 会慢悠悠爬), 不代表趋势
    #斜率 ≥ 2%/4日 → 短期强趋势 (4日窗口, 更敏感, 避免20日滞后)
    if alignment == "多头排列":
        # 多头排列 + MA20 斜率向上 → 真趋势, 给高分
        if ma20_slope_pct >= 2:
            trend_score += 20
        elif ma20_slope_pct >= 1:
            trend_score += 10
        elif ma20_slope_pct >= 0:
            trend_score += 3
        else:
            # 多头排列但 MA20 斜率向下 → 趋势走坏信号
            swing_score += 8
    elif alignment == "交叉纠缠":
        # 交叉纠缠时 MA20 上移/下移都是噪音, 只给中性分
        if ma20_slope_pct >= 2:
            trend_score += 5   # 只给少量, 不因为 MA20 上移就判趋势
        elif ma20_slope_pct >= 1:
            trend_score += 3
        elif ma20_slope_pct >= 0:
            pass  # 0~1% 完全忽略
        elif ma20_slope_pct > -1:
            swing_score += 3
        else:
            swing_score += 8   # MA20 明显下移 + 交叉纠缠 → 更偏震荡/回调
    else:
        # 空头排列时 MA20 斜率向下是常态, 向上是反弹
        if ma20_slope_pct >= 0:
            swing_score += 3  # 空头里 MA20 上移 = 反弹, 偏波段
        elif ma20_slope_pct > -1:
            swing_score += 5
        else:
            swing_score += 12

    # [已删除] ma20_20d_up_streak 评分 — MA20 太平滑, 横盘震荡也会连续上移, 是假信号

    # 3. 波动水平 (ATR14) → 20260904 用户新阈值
    # 新规则: ATR<4% 偏趋势(稳定品种,趋势持仓舒服); ATR≥4% 偏波段(高波动适合快进快出)
    if atr_pct < 4:       # 低/中波动合并 (银行/白马/稳健成长)
        trend_score += 5   # ATR<4% 统一偏趋势+5
    else:                 # 高波动 (用户偏好波段操作) → 偏波段
        swing_score += 10
        # 唯一例外: 高波动 + MA20斜率≥2% + 多头排列 = 真趋势爆发主升, 额外给趋势加分
        if ma20_slope_pct >= 2 and alignment == "多头排列":
            trend_score += 10

    # 4. 近60日涨跌幅度 (净趋势)
    if chg60_pct >= 15:
        trend_score += 15
    elif chg60_pct >= 5:
        trend_score += 8
    elif chg60_pct >= 0:
        trend_score += 2
    elif chg60_pct > -8:
        swing_score += 8
    else:
        swing_score += 4
        trend_score -= 5

    # 5. 连贯性: 正收益日占比 & 平均波段长度
    # 单边 → 正收益日占比 > 0.65 且 平均波段长度 >= 3.5 日 → 趋势票
    if pos_ratio_20 >= 0.65:
        trend_score += 10
    elif pos_ratio_20 >= 0.5:
        pass
    else:
        swing_score += 8
    if avg_run_pos >= 3.5 or avg_run_neg >= 3.5:
        trend_score += 8
    elif avg_run_pos <= 2.2 and avg_run_neg <= 2.2:
        swing_score += 10  # 1-2日就切换 → 典型来回震荡=波段

    # ---- 20260902 用户要求「波段票减轻均线比重」新增 ----
    # 6. 支撑位有效性 + 踩MA20反弹成功率 (波段判定的核心权重, 替代纯均线交叉)
    #    用户原话: 波段票买点在前期低点附近或者踩均线附近买
    #    所以波段票要看「支撑位被反复测试且有效」, 而不是只看MA金叉死叉
    #    20260904 修复: _rebound_count_gx/_total_touch_gx/_support_level_gx 是本函数局部变量,
    #    之前错误用 globals() 检查(永远找不到), 现在直接引用
    touches = _total_touch_gx
    rebounds = _rebound_count_gx
    # 近半年「踩MA20后反弹成功次数」≥3次 → 典型波段性（靠均线/前低买涨）
    if rebounds >= 4:
        swing_score += 18
    elif rebounds >= 3:
        swing_score += 12
    elif rebounds >= 2:
        swing_score += 6
    # 「触及次数多但反弹成功率<50%」 → 趋势在走坏, 波段属性减弱, 不给额外分
    if touches >= 4 and rebounds >= 1:
        rate = rebounds / touches if touches > 0 else 0
        if rate >= 0.7:
            swing_score += 8   # 支撑有效率高, 波段操作胜率高
        elif rate <= 0.4:
            swing_score -= 4   # 支撑老被打穿, 就不是典型波段

    # 7. 支撑位附近的密集回踩: 现价距离支撑位<8% 且ATR中等/高 → 波段属性加分
    if _support_level_gx and _support_level_gx > 0:
        dist_pct = (c - _support_level_gx) / _support_level_gx * 100
        if 0 <= dist_pct <= 8 and (atr_pct >= 2.5):
            swing_score += 5

    # 8. 降低均线交叉纠缠在波段判定的绝对权重 (交叉纠缠原先+25, 改成+15, 让支撑/反弹成分更重)
    #    (已在前面写了，这里通过后续反向扣分对冲掉交叉纠缠占比过高的情形)
    #    如果 swing_score 目前领先但支撑反弹次数少，说明判定主要靠均线纠缠 → 扣除纯均线的伪波段
    if swing_score > trend_score and rebounds < 2 and touches <= 2:
        swing_score -= 8
        trend_score += 4

    # 夹到 [0, 100]
    trend_score = max(0, min(100, int(trend_score)))
    swing_score = max(0, min(100, int(swing_score)))

    # ===== 风格分类 =====
    # 用户风格 → 趋势票=持仓1~2周(5~10交易日) 波段票=持仓2~5天
    # 再设一挡"避坑/不建议持有" (空头排列 or 高波动且无趋势方向)
    avoid_flag = False
    avoid_reason = ""
    if alignment == "空头排列":
        avoid_flag = True
        avoid_reason = "当前空头排列,任何持仓都属于逆势抄底,A股无做空工具,风险极大"
    elif trend == "空头趋势" and signal == "sell":
        avoid_flag = True
        avoid_reason = f"当前处于{trend},且已触发{signal_type},每一次反弹都是离场窗口,不宜新入"
    elif atr_pct >= 7 and alignment == "交叉纠缠":
        avoid_flag = True
        avoid_reason = f"ATR={atr_pct:.1f}%极高波动+均线缠绕,短线情绪博弈强烈,非职业选手勿参与"

    if avoid_flag:
        style_type = "避坑票 · 不建议持有"
        style_tag_color = "down"   # 绿色(A股下跌色)
        hold_days = [0, 0]
        hold_days_text = "空仓观望,不入场"
        style_badge = "⚠ 避坑"
    else:
        # ---------- 20260904 方向区分(📈红上升/📉绿下降) + 立讯精密 diff负偏趋势 Bug修复 ----------
        if len(closes) >= 21 and closes[-21] > 0:
            chg20_pct = (closes[-1] / closes[-21] - 1) * 100
        else:
            chg20_pct = 0
        if trend == "空头趋势":
            tr_dir = "down"
        elif trend == "多头趋势":
            if chg60_pct <= -8:
                tr_dir = "down"
            elif chg60_pct <= 0 and ma20_slope_pct <= 0:
                tr_dir = "neutral"
            else:
                tr_dir = "up"
        else:
            if ma20_slope_pct >= 0.5 and chg60_pct >= 5:
                tr_dir = "up"
            elif ma20_slope_pct <= -0.5 and chg60_pct <= -5:
                tr_dir = "down"
            elif chg20_pct >= 3:
                tr_dir = "up"
            elif chg20_pct <= -3:
                tr_dir = "down"
            else:
                tr_dir = "neutral"

        ma20_ref_here = ma20[-1] if (len(ma20) > 0 and not math.isnan(ma20[-1]) and ma20[-1] > 0) else c
        below_ma20_effective = c < ma20_ref_here * 0.995
        sell_break_signal = (signal == "sell") and ("跌破" in signal_type or "破位" in signal_type or (side == "sell"))
        swing_long_period = below_ma20_effective and (rebounds >= 2 or sell_break_signal)
        diff = trend_score - swing_score

        if force_trend:
            # 文案统一: 方向只通过 style_tag_color(颜色) + style_badge 里的📈📉区分, 主文案不再写"上升/下降"
            style_type = "趋势票 · 好票可长拿(5日强攻)"
            style_tag_color = "up"; style_badge = "📈 趋势票"
            hold_days = [5, 10]
            hold_days_text = "连续5天站上MA5且5日涨幅>10%, 强制判定趋势票, 建议持仓 1～2 周"
        elif diff >= 10 or (diff > 0 and alignment == "多头排列"):
            # 明确趋势票: 颜色+badge带方向, 文案统一
            style_type = "趋势票 · 好票可长拿"
            if tr_dir == "down":
                style_tag_color = "down"; style_badge = "📉 趋势票"
                hold_days = [0, 3]
                hold_days_text = "下降通道中(趋势分占优但方向朝下), 只做左侧超跌抢反弹 0～3 天, 不恋战"
            elif tr_dir == "up":
                style_tag_color = "up"; style_badge = "📈 趋势票"
                hold_days = [5, 10]
                hold_days_text = "上升通道中, 建议持仓 1～2 周（5～10 个交易日, 趋势不坏不出）"
            else:
                style_tag_color = "gold"; style_badge = "➡ 趋势票"
                hold_days = [3, 7]
                hold_days_text = "趋势分占优但无明确方向, 箱体操作 3～7 天"
        elif diff <= -10 or (swing_score > trend_score and (alignment == "交叉纠缠" or trend == "震荡")):
            # 明确波段票
            style_type = "波段票 · 快进快出"
            if tr_dir == "down":
                style_tag_color = "down"; style_badge = "📉 波段票"
                hold_days = [1, 3]
                hold_days_text = "下降波段(波段分占优+方向朝下), 抢超跌反弹 1～3 天, 见好就收"
            elif tr_dir == "up":
                style_tag_color = "up"; style_badge = "📈 波段票"
                hold_days = [2, 5]
                hold_days_text = "上升波段(波段分占优+方向朝上), 顺势操作 2～5 天（有肉就走）"
            else:
                style_tag_color = "gold"; style_badge = "🎯 波段票"
                hold_days = [2, 5]
                hold_days_text = "震荡市波段票, 建议持仓 2～5 天（有肉就走，切勿恋战）"
        else:
            # ===== 模糊区间: swing>=trend 就优先偏波段 =====
            swing_like = swing_long_period or (swing_score >= trend_score)
            score_tie = abs(diff) <= 2
            if score_tie and swing_like:
                style_type = "偏波段票(长周期) · 5~8天波段操作"
                hold_days = [5, 8]
                hold_days_text = f"长周期波段(趋势分{trend_score} vs 波段分{swing_score}, 分差{abs(diff)}分) → 持仓 5～8 天, 波段操作, 不恋战"
                if tr_dir == "down":
                    style_tag_color = "down"; style_badge = "📉 长波段"
                elif tr_dir == "up":
                    style_tag_color = "up"; style_badge = "📈 长波段"
                else:
                    style_tag_color = "gold"; style_badge = "🌊 长波段"
            elif swing_score >= trend_score:
                style_type = "偏波段票 · 建议3天内决策"
                hold_days = [2, 4]
                hold_days_text = f"波段分领先(趋势{trend_score} vs 波段{swing_score}), 偏波段操作, 建议持仓 2～4 天"
                if tr_dir == "down":
                    style_tag_color = "down"; style_badge = "📉 偏波段"
                elif tr_dir == "up":
                    style_tag_color = "up"; style_badge = "📈 偏波段"
                else:
                    style_tag_color = "gold"; style_badge = "🎯 偏波段"
            else:
                # trend > swing 但分差<10 → 偏趋势
                style_type = "偏趋势票 · 趋势未完可拿1周"
                if tr_dir == "down":
                    style_tag_color = "down"; style_badge = "📉 偏趋势"
                    hold_days = [1, 4]
                    hold_days_text = "趋势分略领先但方向朝下, 只做超跌短弹 1～4 天, 等企稳再转趋势持仓"
                elif tr_dir == "up":
                    style_tag_color = "up"; style_badge = "📈 偏趋势"
                    hold_days = [4, 8]
                    hold_days_text = "偏上升趋势, 建议持仓 4～8 个交易日（趋势不坏不出）"
                else:
                    style_tag_color = "gold"; style_badge = "➡ 偏趋势"
                    hold_days = [3, 7]
                    hold_days_text = "趋势分略领先但无明确方向, 箱体操作, 持仓 3～7 天"

    # ===== 风格匹配的止盈 & 止损 (完全按股性推导, 不绑定天数) =====
    # 思路:
    #  趋势票 止盈 = 2 档 (「主兑现位」「趋势不破坏前可看」)
    #  波段票 止盈 = 1 档 (「兑现位」, 到了就走, 不再额外3档拆分)
    #  止损: 波段票 → 以「前期低点 / 支撑位 / MA10」(即典型买点) 为锚, 向下 1ATR 作为 "破了说明买点失效"
    #        趋势票 → MA20 下方 2% 或 近20日低点 做最终保护
    #  【修正2】趋势强度自适应锚定: 超强势→MA5, 强势→MA10, 普通→MA20(趋势)/MA10(波段), 弱势→不操作
    days_min, days_max = hold_days[0], hold_days[1]
    # 趋势强度分级 (用于决定止损锚定均线)
    _trend_str = _trend_strength_level(c, ma5, ma10, ma20)
    _ts_level = _trend_str["level"]            # super / strong / normal / weak
    _ts_anchor_ma = _trend_str["anchor_ma"]    # 实际锚定均线值
    _ts_anchor_label = _trend_str["anchor_label"]  # MA5 / MA10 / MA20
    _ts_break_pct = _trend_str["break_pct"]    # 跌破锚定均线百分之几触发
    if avoid_flag:
        tp_main = c
        tp_extra = c
        tp_single = c
        sl_style = c
        sl_style_pct = 0.0
        stop_loss_anchor = c
        stop_loss_anchor_name = ""
    else:
        hi20_safe = max(highs[-20:]) if len(highs) >= 20 else (c * 1.20)
        # 用近半年股性计算: 近60日正回报q50/q75作为典型上涨幅度
        pos_rets_60 = [r for r in [(closes[-i]/closes[-i-1]-1)*100
                                   for i in range(1, min(60, len(closes)-1))
                                   if closes[-i-1] > 0] if r > 0]
        q50_pos = _quantile(pos_rets_60, 0.50) if pos_rets_60 else (atr_pct * 0.7)
        q75_pos = _quantile(pos_rets_60, 0.75) if pos_rets_60 else (atr_pct * 1.3)
        # 趋势票: 主兑现位 = 现价 + max(q75_pos, ATR*1.2)%; 延伸目标 = 现价 + ATR*2.5%, 或前高
        # 波段票: 兑现位 = 现价 + max(q50_pos, ATR*0.9)% (有肉就走, 不奢望吃大段)
        # 用户偏好: 高波动 → ATR≥3% 的都算偏好匹配, 止盈目标上抬一档
        pref_bonus_highvol = (atr_pct >= 3.0)
        # 辅助: 有效跌破MA20判定(放在这里重新算一次, 保证变量独立可用)
        _ma20_cur = ma20[-1] if (len(ma20) > 0 and not math.isnan(ma20[-1]) and ma20[-1] > 0) else (c * 0.97)
        _below_ma20 = c < _ma20_cur * 0.995
        # 支撑位有效性: support_level 必须是正数且低于现价(否则这个支撑位是历史高点, 无效)
        _support_valid = (isinstance(support_level, (int, float)) and support_level > 0 and support_level < c * 1.01)
        if "趋势" in style_type:
            # 趋势票 2 档止盈 (高波动偏好: 主止盈ATR×1.2→1.5, 延伸ATR×2.5→3.2)
            atr_mul_main = 1.5 if pref_bonus_highvol else 1.2
            atr_mul_extra = 3.2 if pref_bonus_highvol else 2.5
            move_main = max(q75_pos, atr_pct * atr_mul_main, 1.5)
            move_extra = max(move_main * 1.6, atr_pct * atr_mul_extra, 3.5 if pref_bonus_highvol else 3.0)
            tp_main_raw = round(c * (1 + move_main / 100), 2)
            tp_extra_raw = round(c * (1 + move_extra / 100), 2)
            # 主兑现位上限 = hi20_safe (不打折扣, 允许到前高甚至超过 1% 确认突破)
            tp_main = tp_main_raw
            if hi20_safe < c * 1.03:
                tp_main = round(hi20_safe, 2)
            else:
                tp_main = min(tp_main_raw, round(hi20_safe * (1.01 if pref_bonus_highvol else 1.002), 2))
            # 高波动偏好: 延伸位直接 = tp_extra_raw 不以前高压 (允许看突破)
            if pref_bonus_highvol:
                tp_extra = tp_extra_raw
            else:
                tp_extra = max(tp_extra_raw, round(hi20_safe * 1.02, 2))
            tp_single = None
            # ---------- 止损: 按趋势强度自适应锚定 ----------
            if _ts_level == "weak":
                # 弱势/空头: 不操作, 但若已持有则反弹离场, 止损锚用 MA20 做最后防线
                sl_style = round(_ma20_cur * 0.98, 2)
                stop_loss_anchor = _ma20_cur
                stop_loss_anchor_name = f"弱势/空头 → MA20({_ma20_cur:.2f})×0.98 做最后防线, 反弹离场不恋战"
            elif _below_ma20 and _support_valid:
                # 用户指出多氟多的问题: 已经跌破MA20, 就不能再用"MA20下方2%"做止损锚了
                # → 改用波段票的筑底支撑位逻辑 (前期低点聚类支撑位 = 真正的买点锚)
                buy_anchor = min(support_level, _ma20_cur)
                sl_below_anchor1 = round(buy_anchor - atr_abs, 2)
                sl_below_anchor2 = round(buy_anchor * 0.985, 2)
                sl_style = min(sl_below_anchor1, sl_below_anchor2)
                stop_loss_anchor = buy_anchor
                stop_loss_anchor_name = (f"【已跌破MA20→改用筑底锚】支撑位{support_level:.2f}/MA20{_ma20_cur:.2f}取低"
                                         f" (跌破说明筑底失败；不再以MA20为锚)")
            elif _ts_level in ("super", "strong") and _ts_anchor_ma and _ts_anchor_ma > 0:
                # 【修正2】超强势/强势: 用趋势强度分级的锚定均线 (MA5/MA10), 跌破 break_pct% 离场
                # 锁大肉, 不让正常回调洗出; 仍向下再给 1ATR 容忍防假摔
                sl_anchor = _ts_anchor_ma
                sl_below_pct = round(sl_anchor * (1 - _ts_break_pct / 100), 2)
                sl_below_atr = round(sl_anchor - atr_abs, 2)
                sl_style = min(sl_below_pct, sl_below_atr)
                stop_loss_anchor = sl_anchor
                stop_loss_anchor_name = (f"{_trend_str['desc']} → 锚{_ts_anchor_label}({sl_anchor:.2f})"
                                         f"×(1-{_ts_break_pct}%)={sl_below_pct:.2f}, 再取 min(,_ATR{atr_abs:.2f})={sl_style:.2f}")
            else:
                # 正常趋势票(未跌破MA20, 普通多头/震荡): MA20 × 0.98, 与 20日低点 × 0.985 取较小
                sl_ma20_protect = round(_ma20_cur * 0.98, 2)
                lo20_safe = min(lows[-20:]) if len(lows) >= 20 else (c * 0.95)
                sl_lo20_protect = round(lo20_safe * 0.985, 2)
                sl_style = min(sl_ma20_protect, sl_lo20_protect)
                stop_loss_anchor = _ma20_cur
                stop_loss_anchor_name = f"普通多头 → MA20({_ma20_cur:.2f})×0.98={sl_ma20_protect:.2f} (跌破2%触发减仓/清仓)"
            # 安全钳: 高波动可接受, 下限=现价×0.85 (最多容忍15%, 防止极端离谱)
            sl_style = max(sl_style, round(c * 0.85, 2))
        else:
            # 波段票: 1 档止盈, 到了就走 (高波动偏好: ATR×0.9→1.2, 不贪0.9, 吃到1段1.2ATR)
            atr_mul_swing = 1.2 if pref_bonus_highvol else 0.9
            move_one = max(q50_pos, atr_pct * atr_mul_swing, 2.0 if pref_bonus_highvol else 1.5)
            tp_main_raw = round(c * (1 + move_one / 100), 2)
            # 高波动偏好: 止盈不以前高硬性限制(高波动容易冲过前高), 只在当前价离前高很近时夹一下
            if pref_bonus_highvol:
                if (hi20_safe - c) / c * 100 < 1.5:
                    tp_main = min(tp_main_raw, round(hi20_safe * 0.995, 2))
                else:
                    tp_main = tp_main_raw
            else:
                if (hi20_safe - c) / c * 100 > 1.5:
                    tp_main = min(tp_main_raw, round(hi20_safe * 0.992, 2))
                else:
                    tp_main = tp_main_raw
            tp_single = tp_main           # 主兑现位 (=波段唯一位)
            tp_extra = tp_main            # 不再提供额外延伸位 (避免用户恋战)
            # 【修正1+2】止损: 波段票买点锚按个股情况算 + 趋势强度自适应
            #  修正1: 买点锚从 MA20 改为 MA10 (符合用户原始规则"波段票锚定MA10")
            #         买点锚 = min(前期低点支撑位, MA10); 止损 = 买点锚 - ATR (或买点锚的 98.5%)
            #  修正2: 超强势/强势波段票也用 MA5/MA10 锚定, 锁住快涨波段
            ma10_safe = ma10[-1] if (len(ma10) > 0 and not math.isnan(ma10[-1]) and ma10[-1] > 0) else (c * 0.99)
            ma20_safe = ma20[-1] if (len(ma20) > 0 and not math.isnan(ma20[-1]) and ma20[-1] > 0) else (c * 0.99)
            if _ts_level == "weak":
                # 弱势/空头波段: 不操作, 反弹离场
                sl_style = round(ma10_safe * 0.985, 2)
                stop_loss_anchor = ma10_safe
                stop_loss_anchor_name = f"弱势/空头 → MA10({ma10_safe:.2f})×0.985 做最后防线, 反弹离场"
            elif _ts_level in ("super", "strong") and _ts_anchor_ma and _ts_anchor_ma > 0:
                # 【修正2】超强势/强势波段: 用趋势强度分级的锚 (MA5/MA10), 跌破 break_pct% 离场
                sl_anchor = _ts_anchor_ma
                sl_below_pct = round(sl_anchor * (1 - _ts_break_pct / 100), 2)
                sl_below_atr = round(sl_anchor - atr_abs, 2)
                sl_style = min(sl_below_pct, sl_below_atr)
                stop_loss_anchor = sl_anchor
                stop_loss_anchor_name = (f"{_trend_str['desc']} → 锚{_ts_anchor_label}({sl_anchor:.2f})"
                                         f"×(1-{_ts_break_pct}%)={sl_below_pct:.2f}, 再取 min(,_ATR{atr_abs:.2f})={sl_style:.2f}")
            else:
                # 普通波段: 买点锚 = min(支撑位, MA10) [修正1: MA20→MA10]
                if _support_valid:
                    buy_anchor = min(support_level, ma10_safe)
                else:
                    buy_anchor = ma10_safe
                # 止损 = 买点锚 - ATR (或买点锚的 98.5%), 取小
                sl_below_anchor1 = round(buy_anchor - atr_abs, 2)
                sl_below_anchor2 = round(buy_anchor * 0.985, 2)
                sl_style = min(sl_below_anchor1, sl_below_anchor2)
                stop_loss_anchor = buy_anchor
                if _support_valid:
                    stop_loss_anchor_name = f"波段锚点=支撑位{support_level:.2f}/MA10{ma10_safe:.2f}取低 (跌破锚点说明买点失败)"
                else:
                    stop_loss_anchor_name = f"波段锚点=MA10{ma10_safe:.2f} (跌破说明买点失败)"
            # 安全钳: 高波动可接受, 下限=现价×0.85 (最多容忍15%, 防止极端离谱)
            sl_style = max(sl_style, round(c * 0.85, 2))
        # ---------- 铁律: 风格止损 sl_style 必须严格低于保守买点 t3_price (至少便宜5%缓冲) ----------
        # 否则会出现"在买3入场, 一进场就止损"的荒谬情况(用户指出多氟多之前就是这样)
        if t3_price and t3_price > 0 and sl_style >= t3_price * 0.95:
            original_sl = sl_style
            # 修正到 t3_price 下方 5% (给买入后5%的安全缓冲, 正常洗盘不会被洗出)
            sl_style = round(t3_price * 0.95, 2)
            # 下限兜底: 不能低于现价85%
            sl_style = max(sl_style, round(c * 0.85, 2))
            stop_loss_anchor_name = (f"{stop_loss_anchor_name}【买点铁律修正: 原{original_sl}≥买3{t3_price},"
                                     f"强制压到买3下方≈5% → 避免挂单买入即触发止损】")
            stop_loss_anchor = t3_price
        # 统一止损%相对现价
        sl_style_pct = (1 - sl_style / c) * 100 if c > 0 else 0

    # ===== 操作建议 (改: 不再硬绑天数, 止盈按股性档位, 止损按买点锚/MA20锚) =====
    tp_upside_main = (tp_main / c - 1) * 100 if (c > 0 and not avoid_flag) else 0
    tp_upside_extra = (tp_extra / c - 1) * 100 if (c > 0 and not avoid_flag) else 0
    if avoid_flag:
        op_tip = "🚫 " + avoid_reason
        buy_plan = "空仓等待趋势翻转; 出现明确多头排列+买入信号后再评估"
        take_profit_plan = "当前持仓者: 建议按已有持仓建议择机清仓离场"
        stop_loss_plan = "若不幸持有,任何反弹至均线(MA5/MA10)附近都应减仓或清仓"
    elif "趋势" in style_type:
        # 趋势票: 2 档止盈 + 宽止损防被洗
        op_tip = f"✅ 趋势票 · 看趋势不坏持股, 不强制天数; 趋势延续性{trend_score}分, 近60日上涨q75={q75_pos:.1f}%"
        # 20260904 同步 ATR 新档位: <4%=稳波动, ≥4%=高波动
        if atr_pct >= 4.0:
            op_tip += " 🔥 高波动趋势(ATR≥4%), 止盈目标上抬一档, 吃整段"
        else:
            op_tip += f" ✔ 稳波动票(ATR={atr_pct:.1f}%<4%), 趋势平滑好拿, 适合长持"
        # 仓位阈值和 buy_plan 里仍然沿用 ≥4% 控仓(逻辑一致, 无需改)
        if days_max - days_min >= 4:
            hold_range_cn = f"{days_min}～{days_max} 个交易日"
        else:
            hold_range_cn = f"{days_min}～{days_max} 天"
        hold_days_text = f"建议持有周期 {hold_range_cn}（仅作参考; 实际到价就走, 趋势没坏可继续）"
        buy_plan = (f"分两次入场: 先用买1(激进≈{t1_price})或买2(稳健≈{t2_price}, 靠近MA20附近回踩位)"
                    f"建仓 1/2, 回踩 MA20({ma20_ref:.2f})附近确认有效后再补剩余半仓;"
                    f"单笔总仓位 ≤ 总仓 {'20%' if atr_pct >= 4 else '35%'}, "
                    f"{'高波动票仓位不超过2成, 否则震幅受不了' if atr_pct >= 4 else '趋势票仓位可以稍重但不一把梭'}")
        take_profit_plan = (f"趋势票 2 档兑现: ① 主兑现位 {tp_main} (现价上方≈+{tp_upside_main:.1f}%, "
                            f"到价先止盈 1/2 仓位落袋为安); ② 延伸看位 {tp_extra} (≈+{tp_upside_extra:.1f}%, "
                            f"剩余 1/2 仓位用 MA20 做移动止盈: MA20不破就拿, 跌破2根K线收不回立即走; 不强制必须到延伸位)")
        stop_loss_plan = (f"最终止损 {sl_style} (现价下方≈{sl_style_pct:.1f}%)。"
                          f"触发条件二选一: ① 收盘价跌破 {stop_loss_anchor_name.split('(')[0].strip()}{round(stop_loss_anchor,2)}×0.98={sl_style}；"
                          f"② 连续2个交易日收在 MA20({round(stop_loss_anchor,2) if 'MA20' in stop_loss_anchor_name else ma20_ref:.2f}) 下方。"
                          f"趋势票容忍稍宽, 避免正常回踩被洗出")
    else:  # 波段票: 1 档止盈 + 紧止损 (到价位/到时间任一走)
        op_tip = f"🎯 波段票 · 吃到一段立即走, 不恋战; 波段性{swing_score}分, ATR={atr_pct:.1f}%"
        # 20260904 同步 ATR 新档位: <4%=稳波动, ≥4%=高波动
        if atr_pct >= 4.0:
            op_tip += " 🔥 高波动波段(ATR≥4%), 波段止盈目标上抬一档, 目标肉=ATR×1.2"
        else:
            op_tip += f" ⚠ 稳波动票(ATR={atr_pct:.1f}%<4%), 波段弹性偏小, 注意别贪到了不走"
        # 动态持仓周期描述: 使用 hold_days 数组真实天数(长周期波段 max>5 天 → 标注"长周期"
        swing_long_marker = "（长周期波段, 允许走完一轮筑底反弹）" if days_max >= 6 else ""
        if days_max - days_min >= 4:
            hold_range_cn = f"{days_min}～{days_max} 个交易日"
        else:
            hold_range_cn = f"{days_min}～{days_max} 天"
        hold_days_text = (f"建议持有周期 {hold_range_cn} {swing_long_marker}"
                          f"（仅参考; 到了止盈位立即走, 没到止损位也破了就走, 天数只做参考）")
        buy_plan = (f"🎯 波段票买点 = 前期低点附近{support_level:.2f} 或 踩均线 MA20({ma20_ref:.2f}) 附近挂单，"
                    f"对应已给档位中的买2(稳健≈{t2_price}) / 买3(保守≈{t3_price})，"
                    f"**只一次性建仓不补仓**，仓位 ≤ 总仓 {'15%' if atr_pct >= 4 else '20%'}，"
                    f"{'高波动票严格控仓≤15%避免单日大波动爆损;' if atr_pct >= 4 else ''}"
                    f"**不追高**（若现价离买2/买3超过2%就放弃等回踩）")
        if tp_single:
            tgt1 = (tp_single / c - 1) * 100 if c > 0 else 0
            take_profit_plan = (f"波段票 1 档止盈: 主兑现位 {tp_single} (现价上方≈+{tgt1:.1f}%, 对应近60日正收益中位数≈{q50_pos:.1f}%)。"
                                f"到价**一次性清仓**，留小尾巴容易从赚到亏；如果第二天跳空高开越过止盈 3% 以上再留 1/3 看惯性，其余全走。")
        else:
            take_profit_plan = "波段票到止盈位一次性兑现，不拖。"
        stop_loss_plan = (f"严格止损 {sl_style} (现价下方≈{sl_style_pct:.1f}%)。"
                          f"锚定逻辑: {stop_loss_anchor_name}，"
                          f"一旦破位说明「前期低点/踩均线」这个波段买点失败，立即割肉不犹豫；"
                          f"第 {days_max} 天若没到止盈、且收盘仍没站上 MA5，**时间止损也走**（不把短线做成长线）。")

    # ===== 风格判定理由 =====
    feat_parts = []
    feat_parts.append(f"MA20 4日斜率{'+' if ma20_slope_pct>=0 else ''}{ma20_slope_pct:.1f}%（{'向上' if ma20_slope_pct>=0 else '向下'}）")
    feat_parts.append(f"近60日涨跌{'+' if chg60_pct>=0 else ''}{chg60_pct:.1f}%")
    feat_parts.append(f"ATR={atr_pct:.1f}%（{vol_level}）")
    feat_parts.append(f"均线：{alignment} / {trend}")
    if run_lens_pos:
        feat_parts.append(f"平均单边上涨波段{avg_run_pos:.1f}天")
    if not avoid_flag and pos_rets_60:
        feat_parts.append(f"上涨中位数q50={q50_pos:.1f}% q75={q75_pos:.1f}%")
    judge_reason = f"{style_type}。判定依据：{'；'.join(feat_parts)}。"
    if avoid_flag:
        judge_reason += "⚠" + avoid_reason

    # 返回结构: 趋势票用tp_main/tp_extra 2档; 波段票用 tp_single 1档
    my_style = {
        "style_type": style_type,
        "style_badge": style_badge,
        "tag_color": style_tag_color,
        "trend_score": trend_score,
        "swing_score": swing_score,
        "hold_days_min": days_min,
        "hold_days_max": days_max,
        "hold_days_text": hold_days_text,   # 仅参考, 不强制
        # 止盈: 兼容新旧字段
        "tp_main": round(tp_main, 2) if not avoid_flag else round(tp_main, 2),
        "tp_extra": round(tp_extra, 2) if not avoid_flag else round(tp_extra, 2),
        "tp_single": round(tp_single, 2) if (tp_single is not None) else None,
        # 止损
        "stop_loss": round(sl_style, 2),
        "stop_loss_pct": round(sl_style_pct, 2),
        "stop_loss_anchor": round(stop_loss_anchor, 2) if stop_loss_anchor and not avoid_flag else None,
        "stop_loss_anchor_name": stop_loss_anchor_name,
        # 趋势强度分级 (前端展示用, 让用户知道为什么用 MA5/MA10/MA20)
        "trend_strength_level": _ts_level,            # super/strong/normal/weak
        "trend_strength_label": _trend_str["label"],  # 超强势/强势/普通多头/弱势
        "trend_strength_anchor": _ts_anchor_label,    # MA5/MA10/MA20
        "trend_strength_desc": _trend_str["desc"],    # 完整说明
        # 文案
        "op_tip": op_tip,
        "buy_plan": buy_plan,
        "take_profit_plan": take_profit_plan,
        "stop_loss_plan": stop_loss_plan,
        "judge_reason": judge_reason,
        # 辅助(老字段保留防前端报错)
        "tp_short": round(tp_main, 2),
        "tp_mid": round(tp_extra, 2),
        "tp_long": round(tp_extra, 2),
        "extra": _style_extra,
        # 显式展示 ATR (用于高波动股不限制, 只给信息)
        "atr_pct": round(atr_pct, 2),
        "atr_abs": round(atr_abs, 2),
    }
    # ============ 我的持仓风格 END ============

    # 👉 信号升级 (最后一步在return前覆盖):
    #    A) 过热乖离(卖点5) → 减1/3
    #    B) 假死叉(V反) → 观望
    #    C) NEW 20260902 用户要求 波段票 减轻均线比重:
    #         → 波段票 (swing_score>trend_score) 且 收盘站上MA20且≥1% → 即使均线出现死叉, 也降级不做卖点4,
    #            改为观望或买入(如果支撑位也成立即踩前低/踩均线买点匹配).
    #         → 均线死叉只在"真破位"(收盘<MA20×0.995)时触发, 否则波段看支撑/量更重要
    if is_overheat:
        signal = "sell"
        signal_type = "卖点5: 过热止盈"
        if "过热" not in (reason_parts or []):
            reason_parts.append(f"乖离过大(高于MA5 +{bias_to_ma5:.1f}%, 近5日 +{r5:.1f}%)→先落袋1/3")
    elif is_false_death_cross:
        signal = "wait"
        signal_type = "假死叉 · 转为观望"
        reason_parts.append(f"短期均线死叉但股价已站回MA20(+{bias_to_ma20:.1f}%), 是均线滞后的假信号, 先观察不出场")

    # ---- 买点4/卖点2/卖点6/卖点7 仅趋势票触发: style_type确认后, 非趋势票降级为wait ----
    _st_for_check = (my_style or {}).get("style_type", "") or ""
    if signal == "buy" and signal_type.startswith("买点4") and "趋势" not in _st_for_check:
        signal = "wait"
        signal_type = "买点4(仅趋势票) · 非趋势票不触发"
        reason_parts.append("非趋势票, 买点4(多头排列加仓)不触发")
        actions = ["仅趋势票触发买点4, 当前非趋势票, 观望"]
    if signal == "sell" and signal_type.startswith("卖点2") and "趋势" not in _st_for_check:
        signal = "wait"
        signal_type = "卖点2(仅趋势票) · 非趋势票不触发"
        reason_parts.append("非趋势票, 卖点2(跌破MA10)不触发")
    if signal == "sell" and signal_type.startswith("卖点6") and "趋势" not in _st_for_check:
        signal = "wait"
        signal_type = "卖点6(仅趋势票) · 非趋势票不触发"
        reason_parts.append("非趋势票, 卖点6(MA5下穿MA20)不触发")
    if signal == "sell" and signal_type.startswith("卖点7") and "趋势" not in _st_for_check:
        signal = "wait"
        signal_type = "卖点7(仅趋势票) · 非趋势票不触发"
        reason_parts.append("非趋势票, 卖点7(趋势停滞)不触发")

    # ---- 波段票 均线比重 降级 (SWING_MA_IMMUNE) ----
    swing_immune_activated = False
    swing_immune_reason = ""
    if SWING_MA_IMMUNE:
        s = my_style or {}
        style_type = s.get("style_type", "") or ""
        ts = s.get("trend_score", 0) or 0
        ws = s.get("swing_score", 0) or 0
        is_swing = ("波段" in style_type) or (ws > ts + 5)
        price_above_ma = (not math.isnan(ma20[-1])) and c > ma20_today * 1.01 if (ma20_today > 0) else False
        # 支撑成立: 现价∈[支撑位×0.98, 支撑位×1.05]或 踩MA20附近 ∈[MA20×0.98, MA20×1.03]
        near_support = (
            (abs(c - _support_level_gx) / max(_support_level_gx, 1e-6) < 0.05)
            or
            (ma20_today > 0 and (abs(c - ma20_today) / ma20_today) < 0.04)
        )
        if is_swing and price_above_ma:
            # 波段票 + 站上MA20 ≥1% → 均线死叉直接失效
            if signal == "sell" and (signal_type and "死叉" in signal_type):
                swing_immune_activated = True
                swing_immune_reason = f"波段票均线豁免：股价站上MA20 +{bias_to_ma20:.1f}%，均线比重降低（看支撑/量能），死叉不触发离场"
                signal = "wait"
                signal_type = "波段票 · 均线假死叉豁免（看支撑/量能）"
                reason_parts.append(swing_immune_reason)
            # 如果 近2日没有金叉，但MA5马上要上穿（MA5距MA20<1.5%）且 接近支撑位/MA20附近 → 升级成「波段票踩线买点」
            if signal == "wait" and ma20_today > 0:
                ma5_today = ma5[-1] if (len(ma5) > 0 and not math.isnan(ma5[-1])) else 0
                ma5_close_to_cross = (ma5_today > 0) and (abs(ma5_today - ma20_today) / ma20_today) < 0.015
                if ma5_close_to_cross and near_support:
                    signal = "buy"
                    signal_type = "买点: 波段票踩线(前低/MA20回踩)"
                    diff_pct = (ma5_today - ma20_today) / ma20_today * 100
                    reason_parts.append(f"波段票：均线即将金叉(MA5 vs MA20差{diff_pct:+.2f}%，MA5≈{ma5_today:.2f}/MA20≈{ma20_today:.2f})，价格在前期低点/MA20买点锚附近（支撑={_support_level_gx:.2f}）")
                    if not entry_price:
                        entry_price = round(c, 2)
                    if not stop_loss:
                        stop_loss = (my_style or {}).get("stop_loss", None) or round(ma20_today * 0.98, 2)
                    if not target_price:
                        # 直接复用 style 已算好的波段目标位
                        tp = (my_style or {}).get("tp_single") or (my_style or {}).get("tp_main")
                        target_price = round(tp, 2) if tp else round(c * 1.06, 2)

    result = {
        "signal": signal,
        "signal_type": signal_type or "观望",
        "side": side,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "reason": "; ".join(reason_parts) if reason_parts else "—",
        "trend": trend,
        "alignment": alignment,
        "bias": round(bias, 2),
        "vol_status": vol_status,
        "vol_ratio": round(vol_ratio, 2),
        "near_ma20": near_ma20,
        "actions": actions or ["观望"],
        "resonance": resonance,
        "resonance_count": resonance_count,
        "warnings": warnings,
        "motto": motto,
        "macd_dif": round(macd_dif, 4),
        "macd_dea": round(macd_dea, 4),
        "macd_hist": round(macd_hist_t, 4),
        "macd_above_zero": macd_above_zero,
        "kdj_j": round(J_t, 2),
        "hold_view": hold_view,
        # →→ 用户要求: 未持有 只分析买单(统一止损=风格止损, 一个数不搞两个版本); 已持有 只看风格止盈止损
        #     → buy_tiers.unified_stop_loss 强制对齐为 my_style.stop_loss (共享同一个"锚-1ATR/MA20破位"止损位)
        "buy_tiers": (lambda bt: (bt.update({"stop_loss": (my_style or {}).get("stop_loss", bt.get("stop_loss"))}), bt)[1])(buy_tiers or {}),
        "my_style": my_style,
    }
    # ---------- 避坑票最后一道关口: 清掉 entry_price/stop_loss/target_price(旧降级字段) 和 buy_tiers ----------
    # 防止前端降级分支(else里的 bs.entry_price 等判断)"不小心"又把不合理的入场价/止损价渲染出来给用户 (任何反弹都是离场窗口)
    if avoid_flag:
        result["entry_price"] = None
        result["stop_loss"] = None
        result["target_price"] = None
        result["buy_tiers"] = {}
    return result


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

    bs = analyze_buy_sell(bars)

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
        "bs_signal": bs["signal"],
        "bs_side": bs["side"],
        "bs_type": bs["signal_type"],
        "bs_entry": bs["entry_price"],
        "bs_stop": bs["stop_loss"],
        "bs_target": bs["target_price"],
        "bs_reason": bs["reason"],
        "bs_trend": bs["trend"],
        "bs_alignment": bs["alignment"],
        "bs_vol": bs["vol_status"],
        "bs_actions": bs["actions"],
        "bs_resonance": bs.get("resonance", []),
        "bs_resonance_count": bs.get("resonance_count", 0),
        "bs_warnings": bs.get("warnings", []),
        "bs_motto": bs.get("motto", ""),
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
def _set_screen_progress(msg: str) -> None:
    """更新后台筛选进度(线程安全, 容错)。"""
    try:
        with _screen_lock:
            _state["progress"] = msg
    except Exception:  # noqa: BLE001
        pass


def run_screen(conds=None) -> dict:
    if conds is None:
        conds = set(COND_ALL)
    else:
        conds = set(conds)
    t0 = time.time()
    # 1. 全市场快照
    _set_screen_progress("拉取全市场实时行情…")
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
    _set_screen_progress(f"构建板块映射 (候选 {len(candidates)} 只)…")
    _build_board_maps()

    # 4. 并发拉取K线并筛选 (MA250需300根)
    total_cand = len(candidates)
    done_cnt = [0]
    hit_cnt = [0]
    _set_screen_progress(f"并发拉取K线 0/{total_cand} (命中 0)…")

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
        done_cnt[0] += 1
        if not res:
            if done_cnt[0] % 100 == 0 or done_cnt[0] == total_cand:
                _set_screen_progress(f"拉取K线 {done_cnt[0]}/{total_cand} (命中 {hit_cnt[0]})…")
            continue
        g = res["groups"]
        # 必选组: 激活就必须通过
        if any(active_g[m] and not g[m] for m in MANDATORY):
            if done_cnt[0] % 100 == 0 or done_cnt[0] == total_cand:
                _set_screen_progress(f"拉取K线 {done_cnt[0]}/{total_cand} (命中 {hit_cnt[0]})…")
            continue
        # 可选组: 通过数 >= flex_threshold (允许差1组)
        flex_pass = sum(1 for gid in GROUP_LEAVES if flex_active[gid] and g[gid])
        if flex_pass < flex_threshold:
            if done_cnt[0] % 100 == 0 or done_cnt[0] == total_cand:
                _set_screen_progress(f"拉取K线 {done_cnt[0]}/{total_cand} (命中 {hit_cnt[0]})…")
            continue
        results.append(res)
        hit_cnt[0] += 1
        if done_cnt[0] % 100 == 0 or done_cnt[0] == total_cand:
            _set_screen_progress(f"拉取K线 {done_cnt[0]}/{total_cand} (命中 {hit_cnt[0]})…")

    # 完全命中(全部激活组通过) 与 接近满足(可选组差1) 分开
    exact = [r for r in results if n_active > 0 and r["score"] == n_active]
    near = [r for r in results if not (n_active > 0 and r["score"] == n_active)]
    exact.sort(key=lambda x: x["est_20d"], reverse=True)
    near.sort(key=lambda x: (x["score"], x["est_20d"]), reverse=True)
    near = near[:60]  # 控制前端载荷
    _set_screen_progress(f"筛选完成: 命中 {len(exact)} 只, 接近满足 {len(near)} 只")

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
app = FastAPI(title="ARD选股系统")
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
        data = run_screen(c)
        with _screen_lock:
            _state["data"] = data
            _state["ts"] = time.time()
            _state["error"] = None
            _state["progress"] = ""
            _state["last_conds"] = c
        # 每日信号归档 (20260906): 当日重扫覆盖当日记录, 失败不影响主流程
        try:
            _archive_put(bj_now("%Y-%m-%d"), "screen", {
                "updated": data.get("updated"),
                "conds": sorted(c),
                "universe": data.get("universe"), "candidates": data.get("candidates"),
                "matched": data.get("matched"), "near_count": data.get("near_count"),
                "exact": (data.get("exact") or [])[:100],
                "stocks": (data.get("stocks") or [])[:60],
            })
        except Exception as ae:  # noqa: BLE001
            print(f"[archive] 筛选结果归档失败: {ae}", flush=True)
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
            "running": _state["running"], "cached": _state["data"] is not None,
            "storage": _kv_storage_init()}


# ============================================================
# SSE 事件流 (20260906 UI优化配套)
# 每2秒推送一次三个后台模块(选股/均线形态/上试盘)的轻量状态快照,
# 状态无变化时发送 keep-alive 注释行。前端 EventSource 订阅:
#   - 进度文本实时驱动进度条 (原方案只在轮询时更新)
#   - ts 变化时前端才拉取全量数据, 替代"固定间隔盲目轮询"
# 前端保留原有轮询作为断连兜底 (见 index.html connectEvents)
# ============================================================
from fastapi.responses import StreamingResponse as _SSEStreamingResponse
import asyncio as _asyncio


@app.get("/api/events")
async def api_events():
    """SSE: 推送 screen/ma/ssp 三模块轻量状态。data 字段为单行 JSON。"""

    def _screen_snapshot() -> dict:
        with _screen_lock:
            return {"running": _state["running"], "progress": _state["progress"],
                    "ts": round(_state["ts"], 1), "error": _state["error"]}

    def _ma_snapshot() -> dict:
        with _ma_state["lock"]:
            data = _ma_state["data"]
            return {"running": _ma_state["running"], "progress": _ma_state["progress"],
                    "ts": round(_ma_state["ts"], 1), "error": _ma_state["error"],
                    "counts": dict(data["counts"]) if data else {}}

    def _ssp_snapshot() -> dict:
        with _SSP_STATE["lock"]:
            return {"running": _SSP_STATE["running"], "progress": _SSP_STATE["progress"],
                    "scan_ts": round(_SSP_STATE["scan_ts"], 1),
                    "updated": _SSP_STATE["updated"], "error": _SSP_STATE["error"],
                    "mkt_filter": bool(_SSP_STATE["mkt_filter"]),
                    "counts": {"上试盘·新信号": len(_SSP_STATE["new_signals"]),
                               "上试盘·观察池": len(_SSP_STATE["watch_pool"]),
                               "上试盘·已确认": len(_SSP_STATE["confirmed_pool"])}}

    async def _gen():
        try:
            last_line = None
            while True:
                payload = {"screen": _screen_snapshot(), "ma": _ma_snapshot(),
                           "ssp": _ssp_snapshot(), "now": bj_now()}
                line = json.dumps(payload, ensure_ascii=False)
                if line != last_line:
                    yield f"data: {line}\n\n"
                    last_line = line
                else:
                    yield ": keep-alive\n\n"
                await _asyncio.sleep(2)
        except _asyncio.CancelledError:
            # 客户端断开连接, 正常退出
            return

    return _SSEStreamingResponse(_gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


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
    """清空本地文件缓存(所有日期), 触发重新拉取。返回新缓存任务状态。
    20260906 升级为强制刷新: 除磁盘缓存外, 同步清空内存中的搜索索引/
    板块映射/指数K线/行情快照缓存, 确保点一次按钮全部数据重新拉取。"""
    import shutil
    import glob as _glob
    cache_date = _cache_date_for_fetch()
    # 删除所有行情快照缓存(含历史日期, 避免跨日残留)
    if os.path.isdir(CACHE_DIR):
        for fp in _glob.glob(os.path.join(CACHE_DIR, "spot_*.json")):
            try:
                os.remove(fp)
            except OSError:
                pass
    # 删除所有K线缓存目录(含历史日期)
    klines_root = os.path.join(CACHE_DIR, "klines")
    if os.path.isdir(klines_root):
        try:
            shutil.rmtree(klines_root)
        except OSError:
            pass
    # 清空内存筛选结果(关键: 避免新筛选完成前返回旧数据)
    with _screen_lock:
        _state["data"] = None
        _state["ts"] = 0.0
        _state["error"] = None
    # 强制刷新: 同步清空各类内存缓存, 下次访问时全部重建/重拉 (20260906)
    _board_cache["built_at"] = 0.0
    _board_cache["industry"] = {}
    _board_cache["industry2"] = {}
    _board_cache["industry3"] = {}
    _board_cache["concept"] = {}
    _INDEX_KLINE_CACHE.clear()
    _stock_search_cache["items"] = []
    _stock_search_cache["built_at"] = 0.0
    _MARKET_SNAP_CACHE["ts"] = 0.0
    _MARKET_SNAP_CACHE["data"] = None
    if not _state["running"]:
        threading.Thread(target=_run_screen_thread, daemon=True).start()
    return {"ok": True, "cleared": cache_date, "running": _state["running"]}


# ============================================================
# 磁盘缓存自动清理 (20260906)
# K线缓存按日期目录(cache/klines/<YYYYMMDD>/)与快照(spot_<YYYYMMDD>.json)
# 只增不减, 长期运行磁盘会持续膨胀; 启动时与每日定时各清一次,
# 只保留最近 _CACHE_KEEP_DAYS 天(盘前回退最近交易日需要跨日数据, 5天足够宽裕)。
# ============================================================
_CACHE_KEEP_DAYS = 5


def _cleanup_old_cache(keep_days: int = _CACHE_KEEP_DAYS) -> int:
    """删除超过 keep_days 天的K线缓存目录与行情快照文件, 返回删除对象数。"""
    import glob as _glob
    import re as _re
    import shutil
    cutoff = (datetime.now(_BJ_TZ) - timedelta(days=keep_days)).strftime("%Y%m%d")
    removed = 0
    klines_root = os.path.join(CACHE_DIR, "klines")
    if os.path.isdir(klines_root):
        for d in os.listdir(klines_root):
            if _re.fullmatch(r"\d{8}", d or "") and d < cutoff:
                shutil.rmtree(os.path.join(klines_root, d), ignore_errors=True)
                removed += 1
    for fp in _glob.glob(os.path.join(CACHE_DIR, "spot_*.json")):
        m = _re.search(r"spot_(\d{8})\.json$", fp)
        if m and m.group(1) < cutoff:
            try:
                os.remove(fp)
                removed += 1
            except OSError:
                pass
    if removed:
        print(f"[cache] [{bj_now()}] 已清理 {removed} 个过期缓存对象 (保留最近{keep_days}天)", flush=True)
    return removed


def _cache_cleaner_loop():
    """常驻线程: 每天北京时间 09:00 后清理一次过期缓存 (20260906)"""
    last_day = None
    while True:
        try:
            now = datetime.now(_BJ_TZ)
            if now.hour >= 9 and now.strftime("%Y-%m-%d") != last_day:
                _cleanup_old_cache()
                last_day = now.strftime("%Y-%m-%d")
        except Exception as e:  # noqa: BLE001
            print(f"[cache] 清理线程异常: {e}", flush=True)
        time.sleep(600)


# ============================================================
# 个股分析模块
# ============================================================
_stock_search_cache = {"built_at": 0.0, "items": []}
_SEARCH_TTL = 600  # 10分钟(之前1小时太长, 非交易时段抓到空数据会缓存死)


def _to_symbol(code: str) -> str:
    """代码 -> 新浪symbol
    20260906 审计修复: 补充北交所映射(43/83/87/88/92/4/8开头 -> bj前缀),
    此前 920xxx 等北交所代码被错误映射为 sz 前缀, 导致搜索/分析拉不到K线。"""
    code = code.strip()
    if code.startswith(("sh", "sz", "bj")):
        return code
    if code.startswith(("43", "83", "87", "88", "92")) or code.startswith(("4", "8")):
        return "bj" + code
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


def _build_search_index(force: bool = False):
    """构建股票搜索索引(含拼音首字母), 默认缓存10分钟; 空结果不缓存, force=True强制重建"""
    now = time.time()
    if not force and now - _stock_search_cache["built_at"] < _SEARCH_TTL and _stock_search_cache["items"]:
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
    # 空结果不写入缓存 (避免非交易时段 akshare 返回空被缓存 10 分钟搜不到)
    if items:
        _stock_search_cache["items"] = items
        _stock_search_cache["built_at"] = now


class _StockSearchReq(BaseModel):
    q: str = ""


def _do_stock_search(q: str) -> dict:
    """真正执行搜索逻辑(中文不再走URL,避免被代理层拦截报400)
    兜底: 正常走一次缓存后仍空, 强制重建一次再搜 (解决早上启动时抓到空缓存的问题)
    """
    _build_search_index()
    q = (q or "").strip().lower()
    if not q:
        return {"items": []}
    items = _stock_search_cache["items"]

    def _pick(hay: list) -> list:
        code_match = [it for it in hay if q in it["code"].lower()]
        name_match = [it for it in hay if q in it["name"]]
        py_match = [it for it in hay if q in it["py_abbr"] or q in it["py_full"]]
        seen = set()
        result = []
        for it in code_match[:5] + name_match[:5] + py_match[:5]:
            if it["code"] not in seen:
                seen.add(it["code"])
                result.append({"code": it["code"], "name": it["name"], "symbol": it["symbol"]})
            if len(result) >= 10:
                break
        return result

    result = _pick(items)
    # 兜底: 缓存里搜不到 → 强制重建索引再试一次 (1.早上空缓存没写 2.新上市股票可能漏)
    if not result:
        _build_search_index(force=True)
        items2 = _stock_search_cache["items"]
        if items2 and items2 is not items:
            result = _pick(items2)
    return {"items": result}


@app.get("/api/stock/search")
def api_stock_search(q: str = ""):
    """按代码/名称/拼音首字母联想搜索(GET,纯ASCII安全), 返回最多10条"""
    return _do_stock_search(q)


@app.post("/api/stock/search")
def api_stock_search_post(req: _StockSearchReq):
    """POST版搜索, 中文放在JSON body不被HTTP层误判, 返回最多10条"""
    return _do_stock_search(req.q)


# ============================================================
# 个股分析历史记录 (最多10条)
# ============================================================
HISTORY_FILE = os.path.join(CACHE_DIR, "stock_history.json")
_hist_lock = threading.Lock()


def _load_history() -> list:
    with _hist_lock:
        if not os.path.exists(HISTORY_FILE):
            return []
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []


def _save_history(code: str, name: str) -> None:
    with _hist_lock:
        items = []
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                    items = json.load(f)
            except Exception:
                items = []
        # 去重: 移除同 code 的旧记录
        items = [it for it in items if it.get("code") != code]
        items.insert(0, {"code": code, "name": name, "ts": bj_now()})
        # 最多 50 条
        items = items[:50]
        try:
            with open(HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
        except Exception:
            pass


@app.get("/api/stock/history")
def api_stock_history():
    return {"items": _load_history()}


@app.delete("/api/stock/history")
def api_stock_history_clear():
    with _hist_lock:
        try:
            if os.path.exists(HISTORY_FILE):
                os.remove(HISTORY_FILE)
        except Exception:
            pass
    return {"ok": True}


@app.delete("/api/stock/history/{code}")
def api_stock_history_del(code: str):
    """删除单条历史记录"""
    with _hist_lock:
        items = []
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                    items = json.load(f)
            except Exception:
                items = []
        items = [it for it in items if it.get("code") != code]
        try:
            with open(HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    return {"ok": True, "count": len(items)}


@app.get("/api/stock/analyze")
def api_stock_analyze(code: str = "", date: str = ""):
    """个股深度分析: 基本信息 + 技术指标 + 量价关系
    20260906 新增历史时点复盘: 传 date(YYYY-MM-DD) 时, K线截断到该交易日,
    全部指标/买卖点/量价/KDJ体系按'以该日为最后一天'计算。"""
    code = code.strip()
    if not code:
        return JSONResponse({"error": "code is required"}, status_code=400)
    # 历史时点参数校验
    as_of_date = (date or "").strip()[:10]
    if as_of_date:
        try:
            datetime.strptime(as_of_date, "%Y-%m-%d")
        except ValueError:
            return JSONResponse({"error": "date 格式应为 YYYY-MM-DD"}, status_code=400)
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
    # 20260906 历史时点: 传date时拉更长K线(300根)以便截断后仍有足够历史
    bars_all = fetch_kline(symbol, datalen=300 if as_of_date else 120)
    if not bars_all:
        return JSONResponse({"error": f"找不到股票 {code} 或无K线数据"}, status_code=404)
    as_of_date_actual = bars_all[-1].get("day", "")[:10]
    if as_of_date:
        # 截断到选定日期(含当天), 最多保留120根 —— 等价于"回到那天看当时的分析"
        sliced = [b for b in bars_all if (b.get("day") or "")[:10] <= as_of_date]
        if not sliced:
            return JSONResponse({"error": f"{as_of_date} 早于该股票的数据起点({bars_all[0].get('day','')[:10]})"},
                                status_code=404)
        bars = sliced[-120:]
        as_of_date_actual = bars[-1].get("day", "")[:10]
    else:
        bars = bars_all

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
    # ---------- 辅助信号: 均线排列/MA20方向/近5日K线(用于判断"下跌中途 vs 超跌末端") ----------
    bear_alignment = (not math.isnan(ma5[-1])) and ma5[-1] < ma10[-1] < ma20[-1]  # 空头排列
    bull_alignment = (not math.isnan(ma5[-1])) and ma5[-1] > ma10[-1] > ma20[-1]  # 多头排列
    # MA20 斜率(近3日变化率): 负=仍在下行(下跌中途风险高), 绝对值<0.05% ≈ 走平
    ma20_slope_pct = 0.0
    if len(ma20) >= 4 and not math.isnan(ma20[-1]) and not math.isnan(ma20[-4]) and ma20[-4] > 0:
        ma20_slope_pct = (ma20[-1] / ma20[-4] - 1) * 100
    # 近5日是否连续收盘创新低 (阴跌标志)
    new_low_5d = False
    if len(closes) >= 10:
        recent_5_closes = closes[-5:]
        prev_5_low = min(closes[-10:-5])
        new_low_5d = min(recent_5_closes) < prev_5_low * 0.995
    # 低位+KDJ已超卖(J<30)且不再下行 = 末端信号
    low_area = pos_20d < 30
    mid_area = 30 <= pos_20d <= 80
    high_area = pos_20d > 80
    kdj_sold_out = J_t < 30
    kdj_turn_up = not math.isnan(j[-2]) and J_t > j[-2]
    # 技术分析文字描述
    analysis = []
    # 均线分析
    if not math.isnan(ma5[-1]) and not math.isnan(ma10[-1]) and not math.isnan(ma20[-1]):
        if ma5[-1] > ma10[-1] > ma20[-1]:
            analysis.append({"item": "均线趋势", "desc": "5/10/20日均线多头排列，中期趋势向上", "status": "看多"})
        elif ma5[-1] < ma10[-1] < ma20[-1]:
            analysis.append({"item": "均线趋势", "desc": f"5/10/20日均线空头排列，MA20斜率={ma20_slope_pct:+.2f}%（{'仍在下行' if ma20_slope_pct<-0.05 else '开始走平/拐头'}）", "status": "看空"})
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
                analysis.append({"item": "KDJ金叉", "desc": f"J({J_t:.1f})＞K({K_t:.1f})＞D({D_t:.1f}) 三线多头排列，J值上行，金叉形态", "status": "看多"})
            elif J_t < K_t < D_t and J_t < j[-2]:
                analysis.append({"item": "KDJ死叉", "desc": f"J({J_t:.1f})＜K({K_t:.1f})＜D({D_t:.1f}) 三线空头排列，J值下行，死叉形态", "status": "看空"})
    # ========== 风格判定+买卖点 (完整版, 提前算, 供 vp_system/kdj_system 复用同一套风格分数) ==========
    bs = analyze_buy_sell(bars)
    # ========== 量价关系6步量化体系 (提前计算, 供 analysis 列表和前端卡片共用) ==========
    vp_system = None
    try:
        vp_system = calc_vp_system(bars, closes, highs, lows, vols, chgs, ma5, ma10, ma20,
                                   my_style=(bs or {}).get("my_style")) or None
    except Exception:
        vp_system = None
    # ========== KDJ双模式体系 (波段票主武器 × 趋势票扳机 + 周KDJ + 左侧抄底) ==========
    kdj_system = None
    try:
        kdj_system = calc_kdj_system(bars, closes, highs, lows, k, d, j,
                                     ma5, ma10, ma20, wk, wd, wj, vp_system) or None
    except Exception:
        kdj_system = None
    # ========== 量价关系 (按用户上传的6步量化体系) ==========
    # 说明: 具体分析依据(Step1量化定性表格/Step2矩阵/Step3阶段/Step4操作信号/Step5风控否决/Step6盘后流程)
    #       前端从 vp_system 结构化字典渲染为卡片。这里 analysis 列表只保留一条"量价关系"总览项
    #       方便兼容旧技术分析表格的"item=量价关系"展示位。
    if vp_system:
        st = vp_system.get("state") or {}
        sg = vp_system.get("stage") or {}
        vs = vp_system.get("vetoes") or []
        signals = vp_system.get("signals") or []
        tags = vp_system.get("tags") or {}
        # 总状态 + 阶段 + 信号数 + 否决数 → 综合"看多/看空/中性"
        stage_code = sg.get("code", "")
        state_code = st.get("code", "")
        # 综合判多空: 建仓/加仓信号≥1且否决=0 → 看多; 清仓/减仓≥1或否决≥1 → 看空或偏空; 其他中性
        has_buy = any(s["type"] in ("建仓","加仓") for s in signals)
        has_sell = any(s["type"] in ("减仓","清仓") for s in signals)
        if has_sell or vs:
            _vp_status = "看空" if (has_sell and len(vs)>=2) else "偏空"
        elif has_buy and not vs:
            _vp_status = "看多"
        elif stage_code in ("B","C","D"):
            _vp_status = "偏多"
        elif stage_code in ("E","F"):
            _vp_status = "偏空"
        else:
            _vp_status = "中性"
        vp_desc_parts = [
            f"【{st.get('name','')}】{st.get('note','')}",
            f"【{sg.get('name','')}】{sg.get('note','')}",
        ]
        if signals:
            vp_desc_parts.append("操作信号: " + "；".join(f"{s['type']}{s['code']} {s['name']}({s['strength']})" for s in signals[:3]))
        if vs:
            vp_desc_parts.append("⚠️风控否决: " + "；".join(f"{v['code']}{v['name']}" for v in vs[:3]))
        if tags.get("position"):
            vp_desc_parts.append("位置: " + tags["position"])
        if tags.get("trend"):
            vp_desc_parts.append("趋势: " + tags["trend"])
        analysis.append({
            "item": "量价关系",
            "status": _vp_status,
            "desc": " | ".join(vp_desc_parts),
        })
    # ========== 区间位置: 不再单独按百分比打"偏多/偏空"，改为与趋势+KDJ联动打标 ==========
    # 修正之前"位置<30%就盲目标偏多"的误导: 如果还在空头下跌中途, 即使位置低也不给出"偏多"
    if bear_alignment and low_area and (new_low_5d or ma20_slope_pct < -0.05):
        # 下跌中途的低位 = 不是安全区
        pos_status = "中性"
        pos_note = "⚠️ 虽处低位（相对便宜），但空头下跌未止，不是买入窗口"
    elif bull_alignment and low_area:
        # 多头趋势+回踩低位 = 绝佳机会
        pos_status = "看多"
        pos_note = "✅ 多头趋势下回踩低位区，典型回踩买点"
    elif low_area:
        # 低位但不是下跌中途 = 相对偏多
        pos_status = "偏多"
        pos_note = "近20日低位区（相对价位不贵），但需看量价是否确认止跌"
    elif high_area and bull_alignment:
        # 高位但还在多头 = 加速段(看多但注意乖离)
        pos_status = "偏空"
        pos_note = "高位区域（追高盈亏比差），即使趋势向上也不追高，等回踩再进"
    elif high_area:
        pos_status = "偏空"
        pos_note = "近20日高位区，追高盈亏比差，谨防回落"
    else:
        pos_status = "中性"
        pos_note = "处于近20日区间中位，上有压力下有支撑，按趋势跟随"
    analysis.append({
        "item": "区间位置",
        "status": pos_status,
        "desc": f"当前价位处于近20日区间的{pos_20d:.0f}%位置(0%=最低,100%=最高) → {pos_note}",
    })
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
    # 保存到历史记录 (最多10条)
    _save_history(code, name)
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
                 for b, c in zip(bars[-120:], chgs[-120:])],
        # ATR 多周期序列 (用于 K 线图下方 ATR 副图: ATR60/ATR30/ATR14/ATR5)
        "atr_series": (lambda H, L, C: (lambda TRs: {
            p: [round(sum(TRs[max(0,i-p+1):i+1])/min(p, i+1), 3) for i in range(len(TRs))]
            for p in [5, 14, 30, 60]
        })([max(H[i]-L[i], abs(H[i]-C[i-1]) if i>0 else H[i]-L[i], abs(L[i]-C[i-1]) if i>0 else H[i]-L[i]) for i in range(len(C))])
        )(highs, lows, closes) if len(highs)==len(closes) else None,
        "analysis": analysis,
        "bs": bs,
        "vp_system": vp_system,   # 6步量化量价体系分析结果 (用于前端「量价关系」一栏)
        "kdj_system": kdj_system, # KDJ双模式分析结果 (用于前端「KDJ操作手册」一栏)
        "as_of_date": as_of_date_actual,      # 分析基准日(实际K线最后一天)
        "is_historical": bool(as_of_date),    # 是否历史时点复盘
        "requested_date": as_of_date or None, # 用户请求的日期(可能与基准日不同, 如非交易日)
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


def _ma_quality_score(item: dict) -> float:
    """
    均线形态筛选综合排序分 → 贴合"可操作性"而非单纯活跃度。
    维度: 买点强度(bs)×3 + 量价配合(vp)×2 + 趋势排列(trend)×2 + 活跃度(amount)×1
    分数越高 → 越值得优先关注。
    设计要点:
      - 买点强度权重最大(×3): 突破>回踩>金叉>加仓; 卖出信号扣分排末尾。
      - 量价配合(×2): 放量涨/缩量跌(洗盘)为佳; 放量跌(出货)扣分。
      - 趋势排列(×2): 多头排列加分, 空头趋势归零。
      - 活跃度(×1): log归一化, 避免大盘股垄断排序。
    """
    # --- 1. 买点强度 (bs_score: -5 ~ 10) ---
    sig = item.get("bs_signal", "wait")
    stype = item.get("bs_type", "") or ""
    if sig == "buy":
        if "买点2" in stype:          # 上穿突破 (趋势启动, 最强)
            bs_score = 10.0
        elif "买点1" in stype:        # 回踩不破 (多头趋势中买点)
            bs_score = 9.0
        elif "买点3" in stype:        # 金叉买入
            bs_score = 8.0
        elif "买点4" in stype:        # 多头排列加仓 (非开仓首选)
            bs_score = 7.0
        else:
            bs_score = 6.0            # 其他买入信号
        if "过热" in stype:           # 过热警告扣分
            bs_score -= 2.0
    elif sig == "sell":
        bs_score = -5.0               # 卖出信号排末尾
    else:
        bs_score = 0.0                # 观望

    # --- 2. 量价配合度 (vp_score: -3 ~ 12) ---
    vol = item.get("bs_vol", "正常")
    chg = item.get("change_pct", 0)
    if vol == "放量":
        vp_score = 10.0 if chg > 0 else -3.0      # 放量涨=量价齐升 / 放量跌=出货嫌疑
    elif vol == "缩量":
        vp_score = 8.0 if chg < 0 else 5.0         # 缩量跌=洗盘特征 / 缩量涨=动能不足
    else:
        vp_score = 5.0                              # 正常
    res_cnt = item.get("bs_resonance_count", 0) or 0
    vp_score += min(res_cnt, 4) * 0.5               # 指标共振加成, 最多 +2

    # --- 3. 趋势/排列加成 (trend_score: 0 ~ 5) ---
    trend = item.get("bs_trend", "")
    align = item.get("bs_alignment", "")
    if trend == "多头趋势":
        trend_score = 5.0 if align == "多头排列" else (4.0 if align == "短多头" else 3.0)
    elif trend == "震荡":
        trend_score = 2.0
    else:  # 空头趋势
        trend_score = 0.0

    # --- 4. 活跃度 (amount_score: log归一化) ---
    amt = item.get("amount_yi", 0) or 0
    amount_score = math.log10(amt + 1) if amt > 0 else 0   # 1亿≈0.30, 10亿≈1.04, 100亿≈2.01

    # --- 综合分 ---
    total = bs_score * 3 + vp_score * 2 + trend_score * 2 + amount_score * 1
    return round(total, 2)


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
            # 买卖点分析
            bs = analyze_buy_sell(bars)
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
                "bs_signal": bs.get("signal", "wait"),
                "bs_side": bs.get("side", "wait"),
                "bs_type": bs.get("signal_type", "观望"),
                "bs_entry": bs.get("entry_price"),
                "bs_stop": bs.get("stop_loss"),
                "bs_target": bs.get("target_price"),
                "bs_reason": bs.get("reason", ""),
                "bs_actions": bs.get("actions", []),
                "bs_trend": bs.get("trend", ""),
                "bs_alignment": bs.get("alignment", ""),
                "bs_vol": bs.get("vol_status", ""),
                "bs_resonance": bs.get("resonance", []),
                "bs_resonance_count": bs.get("resonance_count", 0),
                "bs_warnings": bs.get("warnings", []),
                "bs_motto": bs.get("motto", ""),
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
            # 综合排序分: 买点强度×3 + 量价配合×2 + 趋势排列×2 + 活跃度×1 (见 _ma_quality_score)
            results[p].sort(key=lambda x: -_ma_quality_score(x))
        out = {
            "patterns": {p: results[p] for p in MA_PATTERNS},
            "counts": {p: len(results[p]) for p in MA_PATTERNS},
            "total_stocks": total,
            "elapsed_sec": round(time.time() - t0, 1),
            "updated": bj_now(),
        }
        # 每日信号归档 (20260906)
        try:
            _archive_put(bj_now("%Y-%m-%d"), "ma", {
                "updated": out["updated"], "counts": out["counts"],
                "total_stocks": total, "elapsed_sec": out["elapsed_sec"],
                "patterns": {p: results[p][:100] for p in MA_PATTERNS},
            })
        except Exception as ae:  # noqa: BLE001
            print(f"[archive] 均线结果归档失败: {ae}", flush=True)
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
            _ma_state["data"] = None
            _ma_state["ts"] = 0.0
            _ma_state["error"] = None
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


# ============================================================
# 上试盘策略模块
#   按《上试盘策略规则说明书 C1》2026-09-04 实现
# ============================================================
_SSP_PARAMS = dict(
    shadow_min=0.03, shadow_ratio=0.50, amp_min=4.0, close_pos=0.40,
    low_pos=1.40, consol_max=0.25, sig_vol=1.5,
    conf_window=10, conf_vol=2.0, brk_buf=0.01,
    stop_buf=0.05, target=0.25, hold_days=40,
)

_SSP_STATE = {
    # 最新扫描结果
    "updated": "",            # YYYY-MM-DD
    "scan_ts": 0.0,           # 实际完成时间戳
    "mkt_filter": True,       # 沪深300 是否在 MA20 上方
    # 三个池
    "new_signals": [],        # 当日新信号 A 池
    "watch_pool": [],         # 待确认观察 B 池 (信号未破位未触发)
    "confirmed_pool": [],     # 已确认买入 C 池（若今天出现确认信号）
    # 状态
    "running": False, "progress": "", "error": None,
    "lock": threading.Lock(),
}
_SSP_CACHE_TTL = 1800  # 30分钟自动重扫; 用户也可手动点击"更新"

def _ssp_compute_features(closes, highs, lows, opens, volumes):
    """返回 ndarrays(填充NaN): prev_close, amp, up_shadow, close_pos, vol_ratio,
    llv120, hhv20, llv20, consol20. 全部对齐, 不足窗口为 NaN。"""
    import numpy as np  # 避免顶层依赖
    n = len(closes)
    c = np.asarray(closes, dtype=float)
    h = np.asarray(highs, dtype=float)
    l = np.asarray(lows, dtype=float)
    o = np.asarray(opens, dtype=float)
    v = np.asarray(volumes, dtype=float)
    nan = float("nan")
    pc = np.concatenate(([nan], c[:-1]))
    rng = h - l; rng_safe = np.where(rng == 0, np.nan, rng)
    amp = rng / np.where(pc > 0, pc, np.nan) * 100
    up_shadow = h - np.maximum(o, c)
    close_pos = (c - l) / rng_safe
    # vol_ratio = 当日量 / 前5日均量 (MA前5日不包含自己)
    def roll_mean(arr, w):
        out = np.full(arr.shape, nan)
        for i in range(w, len(arr)):
            m = arr[i-w:i].mean()
            if np.isfinite(m) and m > 0:
                out[i] = m
        return out
    vma5 = roll_mean(v, 5)
    vol_ratio = np.where(vma5 > 0, v / vma5, nan)
    # llv/hhv
    def roll_window(arr, w, fn):
        out = np.full(arr.shape, nan)
        for i in range(w - 1, len(arr)):
            out[i] = fn(arr[i - w + 1:i + 1])
        return out
    llv120 = roll_window(l, 120, np.nanmin)
    hhv20 = roll_window(h, 20, np.nanmax)
    llv20 = roll_window(l, 20, np.nanmin)
    consol20 = (hhv20 - llv20) / np.where(llv20 > 0, llv20, np.nan)
    return dict(pc=pc, amp=amp, up_shadow=up_shadow, close_pos=close_pos, vol_ratio=vol_ratio,
                llv120=llv120, consol20=consol20)


def _ssp_signal_day_mask(feat, closes):
    """返回信号日(全部7条件) bool ndarray。feat 来自 _ssp_compute_features。"""
    import numpy as np
    P = _SSP_PARAMS
    pc = feat["pc"]
    cond1 = (feat["up_shadow"] / np.where(pc > 0, pc, np.nan)) >= P["shadow_min"]
    rng_safe = feat["up_shadow"]  # 只是占位; 需要 (h-l)
    # 重新算 h-l 比(简化: 用 up_shadow/(close-low) 不合适——这里改用 amp 推, 用 up_shadow/amp 对应条件2)
    #  amp = (h-l)/prev_c ×100, up_shadow/ (h-l) = (up_shadow/pc) / (amp/100)
    ratio_sr = np.where(feat["amp"] > 0,
                        ((feat["up_shadow"] / np.where(pc > 0, pc, np.nan)) / (feat["amp"] / 100)),
                        np.nan)
    cond2 = ratio_sr >= P["shadow_ratio"]
    cond3 = feat["amp"] >= P["amp_min"]
    cond4 = feat["close_pos"] <= P["close_pos"]
    cond5 = closes / np.where(feat["llv120"] > 0, feat["llv120"], np.nan) < P["low_pos"]
    cond6 = feat["consol20"] < P["consol_max"]
    cond7 = feat["vol_ratio"] >= P["sig_vol"]
    import numpy as np
    mask = np.ones(len(closes), dtype=bool)
    for c in (cond1, cond2, cond3, cond4, cond5, cond6, cond7):
        mask &= np.asarray(c, dtype=bool)
    # NaN → False
    mask = np.where(np.isnan(mask.astype(float)), False, mask)
    return mask.astype(bool)


def ssp_single_stock_signals(bars: list[dict], include_confirmed_history=True):
    """单股历史上试盘所有事件:
    返回 {
      'signals':[{day,idx,high,low,close,vol_ratio,shadow_pct,status,  (若确认了: entry_day,entry_price,  exit_day,exit_price,ret_pct,exit_reason)}]
      'active_watch': {sig_high,sig_low,break_price,invalidate_price,remain_days,sig_day} 或 None,
    }
    status: 'new'(最后一日刚出信号) / 'watching'(未破位未触发在窗口内) / 'confirmed'(已确认买入) / 'expired'(超时) / 'broken'(破位作废)
    """
    import numpy as np
    if not bars or len(bars) < 125:
        return {"signals": [], "active_watch": None}
    closes = [b["close"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    opens = [b["open"] for b in bars]
    vols = [b.get("volume", 0) for b in bars]
    feat = _ssp_compute_features(closes, highs, lows, opens, vols)
    mask = _ssp_signal_day_mask(feat, np.asarray(closes, dtype=float))
    P = _SSP_PARAMS
    N = len(bars)
    sig_idx = np.where(mask)[0].tolist()
    out = []
    busy_until = -1
    active_watch = None
    for si in sig_idx:
        if si < busy_until:
            continue
        sig_day = bars[si]["day"]
        sig_h = highs[si]; sig_l = lows[si]; sig_c = closes[si]
        vr = float(feat["vol_ratio"][si]) if np.isfinite(feat["vol_ratio"][si]) else 0.0
        spc = float(feat["up_shadow"][si] / (feat["pc"][si]) * 100) if np.isfinite(feat["up_shadow"][si]) and np.isfinite(feat["pc"][si]) and feat["pc"][si] > 0 else 0
        status = "watching"
        evt = dict(day=str(sig_day), idx=int(si), high=float(sig_h), low=float(sig_l),
                   close=float(sig_c), vol_ratio=round(vr, 2), shadow_pct=round(spc, 2))
        w_end = min(si + 1 + P["conf_window"], N)
        broken_i = None; entry_i = None
        for j in range(si + 1, w_end):
            if closes[j] < sig_l:
                broken_i = j
                break
            if (closes[j] > sig_h * (1 + P["brk_buf"])) and (np.isfinite(feat["vol_ratio"][j]) and feat["vol_ratio"][j] >= P["conf_vol"]):
                entry_i = j
                break
        # 大盘环境过滤在扫描层做, 单股历史回溯不做指数判断(简化)
        if entry_i is None:
            if broken_i is not None:
                status = "broken"
                evt["invalidated_day"] = str(bars[broken_i]["day"])
            else:
                # 窗口走完未确认?
                if w_end >= N:
                    # 最后一日还在窗口内但没确认
                    remain = (N - 1) - si
                    if si == N - 1:
                        status = "new"
                    elif remain < P["conf_window"]:
                        status = "watching"
                        brk_p = round(sig_h * (1 + P["brk_buf"]), 2)
                        inv_p = round(sig_l, 2)
                        active_watch = dict(sig_day=str(sig_day), break_price=brk_p,
                                            invalidate_price=inv_p,
                                            remain_days=P["conf_window"] - remain,
                                            sig_high=float(sig_h), sig_low=float(sig_l))
                    else:
                        status = "expired"
                else:
                    status = "expired"
            evt["status"] = status
        else:
            status = "confirmed"
            entry_day = bars[entry_i]["day"]
            entry_price = float(closes[entry_i])
            stop = sig_l * (1 - P["stop_buf"])
            tgt = entry_price * (1 + P["target"])
            h_end = min(entry_i + 1 + P["hold_days"], N)
            ex_i = None; ex_price = None; reason = "timeout"
            for j in range(entry_i + 1, h_end):
                if lows[j] <= stop:
                    ex_i = j; ex_price = stop; reason = "stop"; break
                if highs[j] >= tgt:
                    ex_i = j; ex_price = tgt; reason = "target"; break
            if ex_i is None:
                ex_i = min(entry_i + P["hold_days"], N - 1)
                ex_price = closes[ex_i]
            ret = (ex_price / entry_price - 1) * 100
            busy_until = ex_i + 1
            evt.update(status="confirmed",
                      entry_day=str(entry_day), entry_price=round(entry_price, 2),
                      exit_day=str(bars[ex_i]["day"]), exit_price=round(float(ex_price), 2),
                      ret_pct=round(ret, 2), exit_reason=reason,
                      stop_price=round(stop, 2), target_price=round(tgt, 2),
                      hold_days=int(ex_i - entry_i))
            evt["status"] = status
        out.append(evt)
    return {"signals": out, "active_watch": active_watch}


def _ssp_hs300_above_ma20(scan_date=None):
    """沪深300 000300 收盘是否在 MA20 上方。失败返回 True(宽松默认通过)。
    20260906 审计修复: 原来传 '000300' 无市场前缀, 新浪/腾讯/东财K线接口都
    无法识别, 大盘过滤形同虚设; 改为带前缀的 sh000300。"""
    try:
        bars = fetch_kline("sh000300", datalen=80)
        if not bars or len(bars) < 25:
            return True
        closes = [b["close"] for b in bars]
        ma20 = sma(closes, 20)
        import math
        # 找 scan_date 对应的最后一根
        last = closes[-1]; m = ma20[-1]
        if not math.isnan(m) and m > 0:
            return bool(last > m)
    except Exception:
        pass
    return True


def _run_ssp_scan_thread():
    """后台全市场扫描: 按均线筛选相同候选池, 产出 A/B/C 三池。"""
    with _SSP_STATE["lock"]:
        if _SSP_STATE["running"]: return
        _SSP_STATE["running"] = True
        _SSP_STATE["error"] = None
        _SSP_STATE["progress"] = "初始化行情…"
    t0 = time.time()
    import math, datetime as _dt
    try:
        # 1) 取 spot + 同样预过滤(ST/科创板/北交所/成交额<1亿排除)
        spot = fetch_spot_all()
        _build_board_maps()
        cands = []
        for r in spot:
            code = r.get("code", ""); name = r.get("name", "")
            if not code or not name: continue
            if "ST" in name or code.startswith(("688", "8", "4")): continue
            try: amt = float(r.get("amount", 0))
            except (TypeError, ValueError): amt = 0
            if amt < 1e8: continue
            cands.append({"code": code, "name": name, "row": r})
        total = len(cands)
        _SSP_STATE["progress"] = f"预筛 {total} 只 · 环境判断中…"
        mkt_ok = _ssp_hs300_above_ma20()
        _SSP_STATE["mkt_filter"] = mkt_ok

        # 2) 逐股拉K线跑识别 → 归类到 A(new)/B(watch)/C(今日确认) 三池
        new_sigs = []
        watch_pool = []
        confirmed_today = []
        done = [0]
        import numpy as np
        P = _SSP_PARAMS

        def proc(cand):
            done[0] += 1
            if done[0] % 40 == 0:
                with _SSP_STATE["lock"]:
                    _SSP_STATE["progress"] = f"扫描中 {done[0]}/{total}"
            try:
                sym = _to_symbol(cand["code"])
                bars = fetch_kline(sym, datalen=140)
                if not bars or len(bars) < 130: return None
                closes = [b["close"] for b in bars]
                highs = [b["high"] for b in bars]
                lows = [b["low"] for b in bars]
                opens = [b["open"] for b in bars]
                vols = [b.get("volume", 0) for b in bars]
                feat = _ssp_compute_features(closes, highs, lows, opens, vols)
                mask = _ssp_signal_day_mask(feat, np.asarray(closes, dtype=float))
                N = len(bars)
                sig_idx = np.where(mask)[0].tolist()
                # 过滤: 信号必须在最后 N 天范围内
                if not sig_idx:
                    return None
                # 最后一日信号 = A 池 (new)
                last_i = N - 1
                today_date = str(bars[-1]["day"])
                out = {"code": cand["code"], "name": cand["name"]}
                today_row = cand.get("row", {}) or {}
                today_pct = today_row.get("change_pct")
                if today_pct is None and len(closes) >= 2:
                    today_pct = round((closes[-1]/closes[-2]-1)*100, 2)
                today_amt = today_row.get("amount") or 0
                try: today_amt_f = round(float(today_amt)/1e8, 2)
                except: today_amt_f = 0
                out.update(price=round(float(closes[-1]),2),
                           change_pct=float(today_pct) if today_pct is not None else None,
                           amount_y=today_amt_f,
                           industry=today_row.get("industry") or get_industry(cand["code"]),
                           concept=today_row.get("concept") or get_concepts(cand["code"]),
                           today=today_date)
                # 分类: 找最后 conf_window 天内的信号
                matched = {"A": None, "B": None, "C": None}
                # 找最近 conf_window + hold_days 内的信号，用于确认池判断
                within = [i for i in sig_idx if i >= N - 1 - P["conf_window"] - P["hold_days"]]
                for si in reversed(within):  # 从新→旧
                    if matched["A"] and matched["B"] and matched["C"]: break
                    sig_day = str(bars[si]["day"])
                    sig_h = highs[si]; sig_l = lows[si]; sig_c = closes[si]
                    vr = float(feat["vol_ratio"][si]) if np.isfinite(feat["vol_ratio"][si]) else 0
                    spc = float(feat["up_shadow"][si] / feat["pc"][si] * 100) if np.isfinite(feat["pc"][si]) and feat["pc"][si] > 0 else 0
                    # 遍历确认窗口
                    w_end = min(si + 1 + P["conf_window"], N)
                    broken_i = None; entry_i = None
                    for j in range(si + 1, w_end):
                        if closes[j] < sig_l: broken_i = j; break
                        if (closes[j] > sig_h * (1 + P["brk_buf"])
                            and np.isfinite(feat["vol_ratio"][j]) and feat["vol_ratio"][j] >= P["conf_vol"]):
                            entry_i = j; break
                    # 情况 A：今日就是信号日本身
                    if si == last_i and matched["A"] is None:
                        rec = dict(out); rec.update(sig_day=today_date,
                            sig_high=round(float(sig_h),2), sig_low=round(float(sig_l),2),
                            shadow_pct=round(spc,2), vol_ratio=round(vr,2),
                            break_price=round(sig_h*(1+P["brk_buf"]),2),
                            invalidate_price=round(sig_l,2))
                        matched["A"] = rec
                    # 情况 B：旧信号仍在窗口内未破位未确认
                    if entry_i is None and broken_i is None and matched["B"] is None:
                        if si != last_i and (N - 1 - si) < P["conf_window"]:
                            rec = dict(out)
                            remain = P["conf_window"] - (N - 1 - si)
                            rec.update(sig_day=sig_day, days_since=int(N-1-si), remain_days=int(remain),
                                sig_high=round(float(sig_h),2), sig_low=round(float(sig_l),2),
                                shadow_pct=round(spc,2), vol_ratio=round(vr,2),
                                break_price=round(sig_h*(1+P["brk_buf"]),2),
                                invalidate_price=round(sig_l,2))
                            matched["B"] = rec
                    # 情况 C：确认日 = 今日
                    # 20260906 用户改: 弱市(沪深300破MA20)不再从确认池剔除股票,
                    # 仅在前端以红色横幅警示 —— 确认信号照常入池供跟踪参考
                    if entry_i == last_i and matched["C"] is None:
                        entry_p = float(closes[entry_i])
                        stop = sig_l * (1 - P["stop_buf"])
                        tgt = entry_p * (1 + P["target"])
                        rec = dict(out); rec.update(sig_day=sig_day, confirm_day=today_date,
                            sig_high=round(float(sig_h),2), sig_low=round(float(sig_l),2),
                            confirm_vol_ratio=round(float(feat["vol_ratio"][entry_i]),2) if np.isfinite(feat["vol_ratio"][entry_i]) else 0,
                            entry_price=round(entry_p,2),
                            stop_price=round(stop,2), target_price=round(tgt,2))
                        matched["C"] = rec
                return matched
            except Exception:
                return None

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            all_matches = [r for r in list(ex.map(proc, cands, timeout=600)) if r]

        for m in all_matches:
            if m.get("A"): new_sigs.append(m["A"])
            if m.get("B"): watch_pool.append(m["B"])
            if m.get("C"): confirmed_today.append(m["C"])

        # 排序: A/B 按信号日从新→旧；C 按 entry 涨幅倒序
        new_sigs.sort(key=lambda x: x.get("change_pct") or 0, reverse=True)
        watch_pool.sort(key=lambda x: (x.get("days_since",99), -abs(x.get("break_price",0) - x.get("price",0))))
        confirmed_today.sort(key=lambda x: x.get("change_pct") or 0, reverse=True)

        # 每日信号归档 (20260906; 用北京时间, 不用服务器本地时区 —— 部署到海外时 date.today() 会错天)
        try:
            _archive_put(bj_now("%Y-%m-%d"), "ssp", {
                "updated": bj_now("%Y-%m-%d"),
                "mkt_filter": mkt_ok,
                "counts": {"上试盘·新信号": len(new_sigs), "上试盘·观察池": len(watch_pool),
                           "上试盘·已确认": len(confirmed_today)},
                "new_signals": new_sigs[:100], "watch_pool": watch_pool[:100],
                "confirmed_pool": confirmed_today[:50],
            })
        except Exception as ae:  # noqa: BLE001
            print(f"[archive] 上试盘结果归档失败: {ae}", flush=True)

        # 20260906 修复: 原写法 `'bars' in dir()` / `'today_date' in dir()` 恒为 False
        # (这两个变量只存在于嵌套函数 proc 的局部作用域, 静态检查也因此报未定义名),
        # 实际效果是 updated 一直取本机日期, 而非本次扫描 K 线的真实交易日。
        # 现改为从扫描结果记录的 today 字段取真实交易日, 无任何命中时回退本机日期。
        trade_date = ""
        for m in all_matches:
            for key in ("A", "B", "C"):
                rec = m.get(key)
                if rec and rec.get("today"):
                    trade_date = rec["today"]
                    break
            if trade_date:
                break
        with _SSP_STATE["lock"]:
            _SSP_STATE["updated"] = trade_date or _dt.date.today().strftime("%Y-%m-%d")
            _SSP_STATE["scan_ts"] = time.time()
            _SSP_STATE["new_signals"] = new_sigs
            _SSP_STATE["watch_pool"] = watch_pool
            _SSP_STATE["confirmed_pool"] = confirmed_today
            _SSP_STATE["progress"] = f"完成 {done[0]}/{total} · 用时 {time.time()-t0:.1f}s"
    except Exception as e:
        with _SSP_STATE["lock"]:
            _SSP_STATE["error"] = f"{type(e).__name__}: {e}"
    finally:
        with _SSP_STATE["lock"]:
            _SSP_STATE["running"] = False


def _ensure_ssp_scan():
    now = time.time()
    with _SSP_STATE["lock"]:
        age = now - _SSP_STATE["scan_ts"]
        if _SSP_STATE["updated"] and age < _SSP_CACHE_TTL and not _SSP_STATE["running"]:
            return
        if _SSP_STATE["running"]: return
    threading.Thread(target=_run_ssp_scan_thread, daemon=True).start()


@app.get("/api/ssp-screen")
def api_ssp_screen():
    """上试盘扫描结果：{updated, running, progress, mkt_filter, new_signals, watch_pool, confirmed_pool, counts}"""
    _ensure_ssp_scan()
    with _SSP_STATE["lock"]:
        running = _SSP_STATE["running"]; progress = _SSP_STATE["progress"]
        err = _SSP_STATE["error"]; updated = _SSP_STATE["updated"]
        mkt = bool(_SSP_STATE["mkt_filter"])
        ns = list(_SSP_STATE["new_signals"]); wp = list(_SSP_STATE["watch_pool"])
        cp = list(_SSP_STATE["confirmed_pool"])
    counts = {"上试盘·新信号": len(ns), "上试盘·观察池": len(wp), "上试盘·已确认": len(cp)}
    # 20260906 用户改: 弱市仅横幅警示, 不再强制清空确认池(移除此前的兜底清空逻辑)
    return JSONResponse({"running": running, "progress": progress, "error": err,
                         "updated": updated, "mkt_filter": mkt,
                         "new_signals": ns, "watch_pool": wp, "confirmed_pool": cp,
                         "counts": counts})


@app.post("/api/ssp-screen/run")
def api_ssp_run():
    """手动触发重新扫描"""
    with _SSP_STATE["lock"]:
        _SSP_STATE["scan_ts"] = 0.0
        _SSP_STATE["data_hint"] = None
    threading.Thread(target=_run_ssp_scan_thread, daemon=True).start()
    return {"started": True}


@app.get("/api/ssp-signals")
def api_ssp_single(code: str):
    """单股历史上试盘事件（用于 K 线图标注信号点）"""
    sym = _to_symbol(code)
    bars = fetch_kline(sym, datalen=180)
    if not bars:
        return JSONResponse({"signals": [], "active_watch": None})
    return ssp_single_stock_signals(bars, True)


# ============================================================
# 历史信号复盘 API (20260906): 每日归档的选股/均线/上试盘结果
# ============================================================
@app.get("/api/history/dates")
def api_history_dates():
    """已归档日期列表 (新→旧)"""
    return {"dates": _archive_dates()}


@app.get("/api/history")
def api_history(date: str = ""):
    """某日归档记录: 选股(screen)/均线(ma)/上试盘(ssp) 三部分; date 缺省取最新一天"""
    date = (date or "").strip()
    if not date:
        ds = _archive_dates()
        date = ds[0] if ds else bj_now("%Y-%m-%d")
    rec = _archive_get(date)
    if rec is None:
        return JSONResponse({"date": date, "record": None,
                             "error": f"{date} 无归档数据 (归档自 20260906 版本起生效)"}, status_code=404)
    return {"date": date, "record": rec}


# ============================================================
# 顶栏指数快照 (上证/深证/创业板 + 行情研判)
# ============================================================
_MARKET_SNAP_CACHE: dict = {"ts": 0.0, "data": None}
_MARKET_SNAP_TTL = 60  # 1 分钟刷新一次 (顶栏实时性不要求秒级)
_MARKET_SNAP_LOCK = threading.Lock()

# 三大指数：symbol, 中文名
_THREE_INDICES = [
    ("sh000001", "上证指数"),
    ("sz399001", "深证成指"),
    ("sz399006", "创业板指"),
]

def _analyze_regime(bars: list[dict]) -> dict:
    """对指数最近 60 个交易日做行情研判.
    返回 {regime, confidence, ma5, ma20, ma60, max60, min60, vol_trend}
    regime in ['上升', '冲高回落', '震荡上行', '震荡', '震荡寻底', '探底回升', '下降']
    """
    import numpy as np
    if len(bars) < 30:
        return {"regime": "数据不足", "confidence": 0,
                "ma5": None, "ma20": None, "ma60": None,
                "ret5": 0, "ret20": 0, "ret60": 0,
                "vol_trend": "平"}
    closes = np.asarray([float(b["close"]) for b in bars], dtype=float)
    highs = np.asarray([float(b["high"]) for b in bars], dtype=float)
    lows = np.asarray([float(b["low"]) for b in bars], dtype=float)
    vols = np.asarray([float(b.get("volume") or 0) for b in bars], dtype=float)
    n = len(closes)

    def _ma(arr, w):
        if n < w: return np.nan
        return float(arr[-w:].mean())
    ma5 = _ma(closes, 5)
    ma20 = _ma(closes, 20)
    ma60 = _ma(closes, 60)

    # 区间涨跌幅
    def _ret(w):
        if n < w+1: return 0.0
        return float((closes[-1]/closes[-w-1]-1)*100)
    r5, r20, r60 = _ret(5), _ret(20), _ret(60)

    # 60 日位置 (0 = 最低, 1 = 最高)
    tail_n = min(60, n)
    h60 = float(highs[-tail_n:].max())
    l60 = float(lows[-tail_n:].min())
    rng60 = max(h60-l60, 1e-9)
    pos60 = (closes[-1]-l60)/rng60

    # 均线斜率 (用最近 N 日线性回归)
    def _slope(arr, w):
        if n < w: return 0.0
        xs = np.arange(w)
        ys = arr[-w:]
        return float(np.polyfit(xs, ys, 1)[0])/float(ys[-1])*100  # % / day
    m20_slp = _slope(closes, 20)
    m5_slp = _slope(closes, 5)

    # 成交量趋势 (近 10 日均量 vs 前 10 日)
    if vols.size >= 20:
        v_recent = float(vols[-10:].mean())
        v_prev = float(vols[-20:-10].mean()) if float(vols[-20:-10].mean())>0 else 1.0
        if v_prev>0 and v_recent / v_prev > 1.25:
            vol_trend = "放量"
        elif v_prev>0 and v_recent / v_prev < 0.8:
            vol_trend = "缩量"
        else:
            vol_trend = "平量"
    else:
        vol_trend = "平量"

    # 近 20 日振幅比例 vs 近 60 日基准 (判断震荡度)
    amp20 = (highs[-20:].max()-lows[-20:].min())/closes[-20] if n>=20 else 0
    amp60 = (h60-l60)/closes[-min(60,n)] if n>=30 else amp20
    narrow = (amp20 / max(amp60, 1e-6)) < 0.65  # 近20振幅显著窄化 = 整理中

    # --- 分类决策 (按优先级由强到弱) ---
    regime = "震荡"
    conf = 0.40

    # 1. 上升: 价格+均线多头 & 斜率向上
    if (closes[-1] > ma20 > ma60 and m20_slp > 0.15 and r20 > 1):
        if m5_slp < -0.1 and r5 < -0.5:
            regime, conf = "冲高回落", 0.72
        elif r60 > 25 and pos60 > 0.92:
            regime, conf = "冲高回落", 0.68
        else:
            regime, conf = "上升", 0.78

    # 2. 下降: 均线空头 & 斜率向下
    elif closes[-1] < ma20 < ma60 and m20_slp < -0.15 and r20 < -1:
        if m5_slp > 0.1 and r5 > 1:
            regime, conf = "探底回升", 0.70
        elif pos60 < 0.08 and r60 < -25:
            regime, conf = "探底回升", 0.65
        else:
            regime, conf = "下降", 0.78

    # 3. 震荡上行: 多头或近20斜率小阳
    elif closes[-1] > ma20 and m20_slp > 0.05:
        regime, conf = ("震荡寻底", 0.55) if narrow and pos60 < 0.3 else ("震荡上行", 0.62)

    # 4. 震荡寻底: 空头 或 接近近期低位
    elif closes[-1] < ma20 and m20_slp < -0.05:
        if pos60 < 0.25 and narrow:
            regime, conf = "震荡寻底", 0.70
        else:
            regime, conf = ("震荡下行", 0.55)

    # 5. 其它: 横盘震荡
    elif narrow:
        regime, conf = "震荡整理", 0.70

    # 量能加权
    if regime in ("上升","震荡上行") and vol_trend == "放量":
        conf = min(0.95, conf + 0.06)
    if regime in ("下降","震荡下行") and vol_trend == "缩量":
        conf = min(0.95, conf + 0.03)

    return {
        "regime": regime,
        "confidence": round(float(conf), 2),
        "ma5": round(ma5, 2) if np.isfinite(ma5) else None,
        "ma20": round(ma20, 2) if np.isfinite(ma20) else None,
        "ma60": round(ma60, 2) if np.isfinite(ma60) else None,
        "ret5": round(r5, 2),
        "ret20": round(r20, 2),
        "ret60": round(r60, 2),
        "pos60": round(float(pos60), 2),
        "vol_trend": vol_trend,
        "narrow_20": bool(narrow),
    }


def _build_index_card(symbol: str, cn_name: str) -> dict | None:
    """构造单只指数卡片数据 (含模拟分时)"""
    import math as _m
    bars60 = _fetch_index_kline(symbol, datalen=70)  # 60+10 富余
    if not bars60 or len(bars60) < 30:
        return None
    # 截取用于研判的 60 日 bars
    ana_bars = bars60[-60:]
    last = bars60[-1]
    prev = bars60[-2] if len(bars60)>=2 else last
    price = float(last["close"])
    prev_close = float(prev["close"])
    open_p = float(last["open"])
    high_p = float(last["high"])
    low_p = float(last["low"])
    chg = price - prev_close
    chg_pct = chg / prev_close * 100

    # 模拟今日分时 (240 点/分钟, 用 正弦波近似: 9:30→11:30, 13:00→15:00)
    # 路径分段: 昨收->开(开盘jump) -> 高/低交错 -> 收盘
    # 为了更真实, 用 4 控制点线性 + 10% 周期扰动
    import numpy as np
    pts = 240
    t = np.linspace(0, 1, pts)
    # 骨架: 0=open, 0.33 = 高或低, 0.66 = 反向极值, 1 = close
    anchors = [open_p, high_p, low_p, price] if high_p != low_p else [open_p, price, price, price]
    # 先看 high 和 low 谁先出现更合理: 如果最终收盘位置偏低, 冲高回落路径; 反之前低后拉
    close_pos = (price - low_p) / max(high_p - low_p, 1e-6)
    if close_pos < 0.5:
        anchors = [open_p, high_p, low_p, price]   # 冲高回落
    else:
        anchors = [open_p, low_p, high_p, price]   # 探底回升收阳
    # 分段线性插值 (0 → 0.3 → 0.7 → 1)
    def _lerp(a,b,x): return a+(b-a)*x
    kts = [0.0, 0.30, 0.70, 1.0]
    base = np.zeros(pts)
    for i,ti in enumerate(t):
        seg = max(0, min(2, int(np.searchsorted(kts[1:], ti, side='right'))))
        a, b = kts[seg], kts[seg+1]
        f = (ti-a)/max(b-a, 1e-9)
        base[i] = _lerp(anchors[seg], anchors[seg+1], f)
    # 叠加平滑小波动 (sin*4 + sin*7, 振幅 ≈ 日内振幅的 5%)
    inner_rng = max(high_p-low_p, 1e-6)
    wav = (0.04 * inner_rng) * (np.sin(t*4*np.pi + 0.7) + 0.6*np.sin(t*7*np.pi + 2.1))
    # 强制端点等于收盘
    wav[-1] = 0
    wav[0] = 0
    curve = base + wav
    curve[0] = open_p
    curve[-1] = price
    # 保证曲线不越日内高低
    curve = np.clip(curve, low_p, high_p)
    regime = _analyze_regime(ana_bars)
    # 小日线: 近 60 日收盘 + 20MA (给 SVG 渲染用, 缩成 120 宽 40 高)
    d60 = ana_bars
    closes60 = [float(b["close"]) for b in d60]
    vol60 = [float(b.get("volume") or 0) for b in d60]
    # 20 日均线窗口
    ma20_l = []
    for i in range(len(closes60)):
        w = min(20, i+1)
        ma20_l.append(sum(closes60[i-w+1:i+1])/w)
    return {
        "symbol": symbol,
        "name": cn_name,
        "price": round(price, 2),
        "prev_close": round(prev_close, 2),
        "open": round(open_p, 2),
        "high": round(high_p, 2),
        "low": round(low_p, 2),
        "chg": round(chg, 2),
        "chg_pct": round(chg_pct, 2),
        "day": str(last.get("day", "")),
        "intraday": [round(float(x), 3) for x in curve.tolist()],
        "series60": {
            "close": [round(x,3) for x in closes60],
            "ma20": [round(x,3) for x in ma20_l],
            "volume": [round(x/1e8,3) for x in vol60],  # 亿
        },
        "regime": regime,
    }


def _aggregate_market_summary(indices: list[dict]) -> dict:
    """把 3 个指数的 regime 汇总成全局一句提示"""
    if not indices:
        return {"overall": "无数据", "color": "gray", "tips": ["指数数据暂无"]}
    # 综合: 三指 regime 出现的加权方向
    UP = {"上升","震荡上行","探底回升"}
    DN = {"下降","震荡下行","冲高回落"}
    MID = {"震荡","震荡整理","震荡寻底","数据不足"}
    score = 0
    for idx in indices:
        r = idx["regime"]["regime"]
        c = idx["regime"]["confidence"]
        if r in UP: score += c
        elif r in DN: score -= c
    # 简单加权
    if score > 0.6:
        overall, color = "偏多 · 轻仓做多", "var(--up)"
    elif score < -0.6:
        overall, color = "偏空 · 严控仓位", "var(--down)"
    else:
        overall, color = "中性 · 观望为主", "var(--gold)"
    tips = [
        f"{idx['name']} {idx['regime']['regime']} (置信 {int(idx['regime']['confidence']*100)}%) · 5日 {'+' if idx['regime']['ret5']>=0 else ''}{idx['regime']['ret5']}% / 20日 {'+' if idx['regime']['ret20']>=0 else ''}{idx['regime']['ret20']}% · 量{idx['regime']['vol_trend']}"
        for idx in indices
    ]
    return {"overall": overall, "color": color, "tips": tips}


_MARKET_SNAP_CACHE = {"ts": 0.0, "data": None}

@app.get("/api/market-snapshot")
def api_market_snapshot():
    """顶栏三大指数 + 行情研判 (每分钟级缓存)."""
    global _MARKET_SNAP_CACHE
    now = time.time()
    with _MARKET_SNAP_LOCK:
        cache = _MARKET_SNAP_CACHE
        if cache.get("data") and (now - cache.get("ts",0)) < _MARKET_SNAP_TTL:
            return JSONResponse(cache["data"])
    cards = []
    for sym, name in _THREE_INDICES:
        try:
            c = _build_index_card(sym, name)
            if c: cards.append(c)
        except Exception as e:
            pass
    summary = _aggregate_market_summary(cards)
    # 市场广度: 涨跌家数 / 涨停跌停 / 总成交额 (基于 spot 快照, 有缓存不慢)
    breadth = {"up": 0, "down": 0, "flat": 0, "limit_up": 0, "limit_down": 0, "amount_yi": 0.0}
    try:
        spot = fetch_spot_all()
        for r in spot:
            try:
                pct = float(r.get("changepercent") or 0)
                if pct > 0: breadth["up"] += 1
                elif pct < 0: breadth["down"] += 1
                else: breadth["flat"] += 1
                code = str(r.get("code", ""))
                if pct >= 9.8:
                    if code.startswith(("300","301","688")):
                        if pct >= 19.5: breadth["limit_up"] += 1
                    else:
                        breadth["limit_up"] += 1
                elif pct <= -9.8:
                    if code.startswith(("300","301","688")):
                        if pct <= -19.5: breadth["limit_down"] += 1
                    else:
                        breadth["limit_down"] += 1
                breadth["amount_yi"] += float(r.get("amount") or 0) / 1e8
            except (ValueError, TypeError):
                continue
        breadth["amount_yi"] = round(breadth["amount_yi"], 1)
    except Exception:
        pass
    data = {
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "indices": cards,
        "summary": summary,
        "breadth": breadth,
    }
    with _MARKET_SNAP_LOCK:
        _MARKET_SNAP_CACHE = {"ts": time.time(), "data": data}
    return JSONResponse(data)


# ============================ 回测引擎 ============================
def _is_limit_up(bar: dict, prev_close: float) -> bool:
    """涨停判定: 主板≥9.8%, 创业板/科创板≥19.5%"""
    if prev_close <= 0:
        return False
    pct = (bar["close"] / prev_close - 1) * 100
    code = bar.get("symbol", "")
    if code.startswith(("sz30", "sh688", "sz301")):
        return pct >= 19.5
    return pct >= 9.8


def _is_limit_down(bar: dict, prev_close: float) -> bool:
    """跌停判定"""
    if prev_close <= 0:
        return False
    pct = (bar["close"] / prev_close - 1) * 100
    code = bar.get("symbol", "")
    if code.startswith(("sz30", "sh688", "sz301")):
        return pct <= -19.5
    return pct <= -9.8


def _run_backtest_single(symbol: str, bars: list, mode: str) -> list:
    """单只股票回测, 返回交易记录列表"""
    trades = []
    if len(bars) < 70:
        return trades
    position = None  # {entry_day, entry_price, stop_loss, target, signal_type, qty}
    for i in range(60, len(bars)):
        bar = bars[i]
        bar["symbol"] = symbol
        prev_close = bars[i - 1]["close"] if i > 0 else bar["close"]
        # ===== 持仓中: 检查出场 =====
        if position:
            exit_price = None
            exit_reason = ""
            # 1. 止损: 当日最低触及止损线
            if position["stop_loss"] and bar["low"] <= position["stop_loss"]:
                exit_price = position["stop_loss"]
                exit_reason = f"止损({position['stop_loss']})"
            # 2. 止盈: 当日最高触及目标价
            elif position["target"] and bar["high"] >= position["target"]:
                exit_price = position["target"]
                exit_reason = f"止盈({position['target']})"
            # 3. 卖出信号
            if not exit_price:
                slice_bars = [dict(b, symbol=symbol) for b in bars[:i + 1]]
                bs = analyze_buy_sell(slice_bars)
                if bs.get("signal") == "sell":
                    # 跌停不可卖 → 顺延
                    if _is_limit_down(bar, prev_close):
                        pass  # 不出场, 等下一天
                    else:
                        exit_price = bar["close"]
                        exit_reason = bs.get("signal_type", "卖出信号")
            if exit_price:
                # 跌停不可卖 → 顺延到下一可卖日
                if _is_limit_down(bar, prev_close) and exit_reason.startswith("止损"):
                    pass
                else:
                    pnl = (exit_price / position["entry_price"] - 1) * 100
                    hold_days = i - position["entry_idx"]
                    trades.append({
                        "code": symbol,
                        "signal_type": position["signal_type"],
                        "entry_day": position["entry_day"],
                        "entry_price": position["entry_price"],
                        "exit_day": bar.get("day", ""),
                        "exit_price": round(exit_price, 2),
                        "pnl_pct": round(pnl, 2),
                        "hold_days": hold_days,
                        "exit_reason": exit_reason,
                        "mode": mode,
                    })
                    position = None
        # ===== 空仓: 检查入场 =====
        if not position:
            slice_bars = [dict(b, symbol=symbol) for b in bars[:i + 1]]
            bs = analyze_buy_sell(slice_bars)
            if bs.get("signal") == "buy":
                # 涨停不可买 → 跳过
                if _is_limit_up(bar, prev_close):
                    continue
                st = bs.get("signal_type", "")
                # 只认买点1-4
                if not any(st.startswith(f"买点{n}") for n in (1, 2, 3, 4)):
                    continue
                entry_price = bar["close"]
                my_style = bs.get("my_style") or {}
                if mode == "band":
                    stop_loss = my_style.get("stop_loss") or bs.get("stop_loss")
                    target = my_style.get("tp_single") or bs.get("target_price") or round(entry_price * 1.06, 2)
                else:  # trend
                    # 趋势模式: 用锚定均线×(1-break_pct), 用my_style的stop_loss
                    stop_loss = my_style.get("stop_loss") or bs.get("stop_loss")
                    target = my_style.get("tp_main") or bs.get("target_price") or round(entry_price * 1.15, 2)
                position = {
                    "entry_day": bar.get("day", ""),
                    "entry_idx": i,
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "target": target,
                    "signal_type": st,
                }
    # 回测结束时若仍持仓, 按最后一日收盘平仓
    if position:
        last = bars[-1]
        exit_price = last["close"]
        pnl = (exit_price / position["entry_price"] - 1) * 100
        trades.append({
            "code": symbol,
            "signal_type": position["signal_type"],
            "entry_day": position["entry_day"],
            "entry_price": position["entry_price"],
            "exit_day": last.get("day", ""),
            "exit_price": round(exit_price, 2),
            "pnl_pct": round(pnl, 2),
            "hold_days": len(bars) - 1 - position["entry_idx"],
            "exit_reason": "回测截止平仓",
            "mode": mode,
        })
    return trades


def _backtest_summary(trades: list) -> dict:
    """汇总统计: 按买点分组 + 整体"""
    def _stats(sub):
        if not sub:
            return {"count": 0, "win_rate": 0, "avg_pnl": 0, "avg_win": 0,
                    "avg_loss": 0, "profit_factor": 0, "max_loss": 0, "avg_hold": 0}
        wins = [t for t in sub if t["pnl_pct"] > 0]
        losses = [t for t in sub if t["pnl_pct"] <= 0]
        avg_pnl = sum(t["pnl_pct"] for t in sub) / len(sub)
        avg_win = sum(t["pnl_pct"] for t in wins) / len(wins) if wins else 0
        avg_loss = abs(sum(t["pnl_pct"] for t in losses) / len(losses)) if losses else 0
        return {
            "count": len(sub),
            "win_rate": round(len(wins) / len(sub) * 100, 1),
            "avg_pnl": round(avg_pnl, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "profit_factor": round(avg_win / avg_loss, 2) if avg_loss > 0 else 0,
            "max_loss": round(min(t["pnl_pct"] for t in sub), 2),
            "avg_hold": round(sum(t["hold_days"] for t in sub) / len(sub), 1),
        }
    # 按买点分组
    by_signal = {}
    for t in trades:
        key = t["signal_type"].split(":")[0].strip() if ":" in t["signal_type"] else t["signal_type"]
        by_signal.setdefault(key, []).append(t)
    summary = {k: _stats(v) for k, v in by_signal.items()}
    summary["整体"] = _stats(trades)
    return summary


@app.post("/api/backtest")
def api_backtest(payload: dict):
    """回测接口
    输入: {stocks: [code...], mode: 'band'|'trend', datalen: 300}
    """
    stocks = payload.get("stocks") if isinstance(payload, dict) else None
    mode = (payload or {}).get("mode", "band")
    datalen = int((payload or {}).get("datalen", 300))
    if not isinstance(stocks, list) or not stocks:
        return JSONResponse({"ok": False, "msg": "stocks必须为非空数组"}, status_code=400)
    if mode not in ("band", "trend"):
        return JSONResponse({"ok": False, "msg": "mode必须为band或trend"}, status_code=400)
    all_trades = []
    errors = []
    for code in stocks[:50]:  # 上限50只
        code = str(code).strip()
        if not code:
            continue
        try:
            symbol = _to_symbol(code)
            bars = fetch_kline(symbol, datalen=datalen)
            if len(bars) < 70:
                errors.append(f"{code}: K线不足70根")
                continue
            trades = _run_backtest_single(symbol, bars, mode)
            all_trades.extend(trades)
        except Exception as e:
            errors.append(f"{code}: {str(e)[:80]}")
    summary = _backtest_summary(all_trades)
    # 累计收益曲线(按时间排序, 等权汇总)
    sorted_trades = sorted(all_trades, key=lambda x: x["exit_day"])
    curve = []
    cum = 0.0
    for t in sorted_trades:
        cum += t["pnl_pct"]
        curve.append({"day": t["exit_day"], "cum_pnl": round(cum, 2)})
    return {
        "ok": True,
        "mode": mode,
        "stock_count": len(stocks),
        "trade_count": len(all_trades),
        "trades": all_trades,
        "summary": summary,
        "curve": curve,
        "errors": errors,
    }


# ============================ 个股分析 单股回测引擎 ============================
INIT_CAPITAL = 100_000.0     # 总资金 10万
STD_POS_CASH = 10_000.0     # 标准档 ≈1万
BUY_COMM = 0.00025          # 买入佣金万2.5
SELL_COMM = 0.00025         # 卖出佣金万2.5
SELL_STAMP = 0.001          # 卖出印花税千1
SELL_FEE_RATE = SELL_COMM + SELL_STAMP


def _round_shares(value):
    """A股 1手=100股, 向下取整到100股整数倍"""
    return int(max(0, int(value) // 100 * 100))


def _calc_shares_by_cash(price, cash, target_cash=None):
    """按目标金额算买入股数(100股整数倍), 实际使用现金和手续费"""
    if price <= 0 or cash <= 0:
        return 0, 0.0, 0.0
    budget = target_cash if (target_cash and target_cash < cash) else cash
    # 预留手续费(预算 × 佣金率), 确保 cash 够
    net_budget = budget / (1.0 + BUY_COMM)
    shares = _round_shares(net_budget / price)
    if shares <= 0 and cash >= price * 100 * (1 + BUY_COMM):
        shares = 100  # 至少能买1手就买1手
    if shares <= 0:
        return 0, 0.0, 0.0
    cost_amt = shares * price
    fee = round(cost_amt * BUY_COMM, 2)
    return shares, round(cost_amt, 2), fee


def _run_single_stock_backtest(symbol: str, bars: list,
                               start_date: str = "", end_date: str = "") -> dict:
    """单股回测引擎
    - T+0.5 当日收盘价成交 (买入/卖出/止盈/止损均于信号产生当日收盘执行, 先卖后买; 当日买入不可当日卖出)
    - 满仓档(买点1/3 + 放量 + 多头趋势)≈10万, 标准档≈1万; 100股整数手
    - 止盈止损: 完全复用 analyze_buy_sell 返回的风格止盈/止损 (方案A)
      · 趋势票: tp_main 到先卖半仓, 剩余半仓 MA20连续2日跌破立即走 / 或到 tp_extra 全卖
      · 波段票: tp_single 全卖(跳空高开越过止盈3%+时简化为直接全卖, 不额外留尾仓)
      · 止损: 跌破 sl_style 立即全卖该笔
      · 卖点信号: 卖点1-4全清仓; 卖点5(过热止盈)减总持仓1/3
    - 手续费: 双边累计
    - 涨跌停: 涨停不开仓, 跌停不平仓
    返回: {stats, closed_trades, open_positions, equity_curve, bars_window, buy_markers, sell_markers}
    """
    n = len(bars)
    if n < 70:
        return {"ok": False, "msg": "K线不足70根"}

    # —— 定位交易窗口 [start_idx, end_idx] 前留预热根数 ——
    # 预热根数: 20 根 (保证 MA20 在回测区间内首日就有有效值)
    PREWARM = 20
    if not end_date:
        end_date = bars[-1].get("day", "")
    if not start_date:
        # 默认3个月前 (往前数约63个交易日 ≈ 3个月)
        start_idx = max(PREWARM, n - 63)
        start_date = bars[start_idx].get("day", "")
    # 找首尾索引 (若用户输入日期不在交易日, 取附近)
    # 用户指定的 start_date 是"回测开始日", 但 MA20 需要前 20 根预热
    # 所以实际 start_idx 可以前移, 只要前面有足够预热根数即可
    _user_start_idx = None
    end_idx = None
    for i, b in enumerate(bars):
        d = b.get("day", "")
        if _user_start_idx is None and d >= start_date and i >= PREWARM:
            _user_start_idx = i
        if d <= end_date:
            end_idx = i
    if _user_start_idx is None:
        _user_start_idx = PREWARM
    if end_idx is None or end_idx < _user_start_idx:
        end_idx = n - 1
    # 最终 start_idx = 用户开始日 (预热数据在 bars[:start_idx] 中, analyze_buy_sell 用全量切片)
    start_idx = _user_start_idx

    # —— 初始化 ——
    cash = INIT_CAPITAL
    total_fees = 0.0
    # positions: [{entry_idx, entry_day, entry_price, shares, style_type,
    #              tp_main, tp_extra, tp_single, sl_style, buy_signal_type,
    #              remaining_shares, half_done, cont_ma_below_days, entry_fee}]
    positions = []
    closed_trades = []
    equity_curve = []  # [{day, equity, benchmark}]
    buy_markers = []   # {idx (相对window), price, day, signal_type, shares, cost}
    sell_markers = []  # {idx, price, day, reason, shares, pnl, fee}

    # 基准 buy_hold: start_date 次日开盘全仓买入, end_date 收盘卖出
    bh_entry_idx = start_idx if (start_idx + 1 > n - 1) else start_idx + 1
    if bh_entry_idx >= n:
        bh_entry_idx = n - 1
    bh_entry_price = bars[bh_entry_idx]["open"]
    bh_shares = _round_shares(INIT_CAPITAL * 0.99 / bh_entry_price) if bh_entry_price > 0 else 0
    bh_cost = bh_shares * bh_entry_price
    bh_fee = round(bh_cost * BUY_COMM, 2)
    bh_cash = INIT_CAPITAL - bh_cost - bh_fee

    for i in range(start_idx, end_idx + 1):
        bar = bars[i]
        prev_close = bars[i - 1]["close"] if i > 0 else bar["close"]
        bar["symbol"] = symbol

        # === T+0: 当日收盘价成交 (买入/卖出/止盈/止损均当日收盘执行, 先卖后买) ===
        close_price = bar["close"]
        # 涨跌停判断 (当日收盘相对昨收, 用于: 涨停不开仓 / 跌停不平仓)
        limit_up = _is_limit_up({"close": close_price, "symbol": symbol}, prev_close) \
            if prev_close > 0 else False
        limit_down = _is_limit_down({"close": close_price, "symbol": symbol}, prev_close) \
            if prev_close > 0 else False

        # -- 收盘后分析当日信号 --
        slice_bars = [dict(x, symbol=symbol) for x in bars[:i + 1]]
        bs = analyze_buy_sell(slice_bars)
        signal = bs["signal"]
        signal_type = bs.get("signal_type", "")
        style = bs.get("my_style") or {}
        style_type = style.get("style_type", "") or ""

        # -- 预备 MA / 当日OHLC (用于止盈止损触发判断) --
        closes = [x["close"] for x in bars[:i + 1]]
        def _sma(arr, w):
            if len(arr) < w:
                return float("nan")
            return sum(arr[-w:]) / w
        ma20_today = _sma(closes, 20)
        ma20_y = _sma(closes[:-1], 20) if len(closes) > 1 else float("nan")
        c_today = bar["close"]
        h_today = bar["high"]
        l_today = bar["low"]

        # === 1. 先执行卖出 (当日收盘价, 释放资金) ===
        # 收集今日卖出动作: (act_src, act_val, reason)  act_val=-1全清, 否则股数
        todays_sells = []

        # -- 单笔止盈/止损 (当日 high/low 触发, 当日收盘价执行) --
        for p in positions:
            # T+0.5: 当日买入的仓位不可当日卖出
            if p["entry_idx"] == i:
                continue
            # 止损: 当日最低触及 sl_style
            if p["sl_style"] and l_today <= p["sl_style"]:
                todays_sells.append(("all", -1, f"跌破风格止损({p['sl_style']})"))
                continue  # 一笔全卖, 不再检查本笔其他条件
            is_swing = bool(p["tp_single"])
            if is_swing:
                # 波段票: 唯一止盈位 tp_single → 全清
                if p["tp_single"] and h_today >= p["tp_single"]:
                    todays_sells.append(("all", -1, f"到波段止盈位({p['tp_single']})"))
            else:
                # 趋势票
                # 止盈1 tp_main: 先卖半仓 (只触发一次)
                if p["tp_main"] and (not p["half_done"]) and h_today >= p["tp_main"]:
                    half = _round_shares(p["remaining_shares"] / 2)
                    if half > 0:
                        todays_sells.append(("all", half, f"到趋势止盈1({p['tp_main']})落袋半仓"))
                    p["half_done"] = True
                # 止盈2 tp_extra → 卖剩余
                if p["tp_extra"] and h_today >= p["tp_extra"]:
                    todays_sells.append(("all", -1, f"到趋势延伸位({p['tp_extra']})清剩余"))
                # 连续2日 MA20 跌破 (更保守保护, 全时检查)
                if p["tp_main"] is not None:  # 趋势票特征
                    today_below = (not math.isnan(ma20_today)) and (c_today < ma20_today)
                    y_below = (not math.isnan(ma20_y)) and (len(closes) > 1 and closes[-2] < ma20_y)
                    if today_below and y_below:
                        todays_sells.append(("all", -1, f"连续2日收在MA20下方(止损剩余)"))

        # -- 卖点信号 (当日收盘价执行; 只在有持仓时触发; T+0.5: 当日买入不可卖) --
        _sellable_positions = [p for p in positions if p["entry_idx"] < i]
        if signal == "sell" and _sellable_positions:
            _sellable_total = sum(p["remaining_shares"] for p in _sellable_positions)
            if "卖点4" in signal_type and "趋势" in style_type:
                # 趋势票 MA5下叉MA10 → 减半仓
                to_sell = _round_shares(_sellable_total / 2)
                if to_sell > 0:
                    todays_sells.append(("all", to_sell, "卖点4: 趋势票MA5下叉MA10减半仓(收盘价)"))
            elif "卖点6" in signal_type:
                # 趋势票 MA5下穿MA20 → 清仓
                todays_sells.append(("all", -1, "卖点6: MA5下穿MA20清仓(收盘价)"))
            elif "卖点5" in signal_type or "过热止盈" in signal_type:
                # 过热止盈 → 减总持仓1/3
                to_sell = _round_shares(_sellable_total / 3)
                if to_sell > 0:
                    todays_sells.append(("all", to_sell, "卖点5过热止盈(减1/3)"))
            else:
                # 卖点1/2/3/7: 全清
                todays_sells.append(("all", -1, signal_type or "系统卖出信号"))

        # -- 实际执行卖出 (当日收盘价; T+0.5: 当日买入仓位跳过) --
        if not limit_down and todays_sells and positions:
            for act_src, act_val, reason in todays_sells:
                if act_val == -1:
                    # 全清: 只清可卖仓位(排除当日买入)
                    sell_shares = sum(p["remaining_shares"] for p in positions if p["entry_idx"] < i)
                elif act_src == "pos":
                    # 指定单笔
                    sell_shares = act_val
                else:
                    sell_shares = min(act_val, sum(p["remaining_shares"] for p in positions if p["entry_idx"] < i))
                if sell_shares <= 0 or not positions:
                    continue
                # 先入先出 从老位置扣 (跳过当日买入仓位)
                sell_remaining = sell_shares
                for p in list(positions):
                    if sell_remaining <= 0:
                        break
                    if p["entry_idx"] == i:
                        continue  # T+0.5: 当日买入不可卖
                    avail = p["remaining_shares"]
                    if avail <= 0:
                        continue
                    take = min(avail, sell_remaining)
                    pnl = (close_price - p["entry_price"]) * take
                    pnl_pct = (close_price / p["entry_price"] - 1) * 100
                    revenue = round(take * close_price, 2)
                    fee = round(revenue * SELL_FEE_RATE, 2)
                    cash += revenue - fee
                    total_fees += fee
                    p["remaining_shares"] -= take
                    sell_remaining -= take
                    hold_days = i - p["entry_idx"]
                    closed_trades.append({
                        "entry_day": p["entry_day"],
                        "exit_day": bar.get("day", ""),
                        "entry_price": p["entry_price"],
                        "exit_price": round(close_price, 2),
                        "shares": take,
                        "pnl_amt": round(pnl, 2),
                        "pnl_pct": round(pnl_pct, 2),
                        "hold_days": hold_days,
                        "fee": fee,
                        "buy_signal_type": p["buy_signal_type"],
                        "exit_reason": reason,
                        "style_type": p["style_type"],
                    })
                    # marker: 相对窗口索引
                    rel_idx = i - start_idx
                    sell_markers.append({
                        "idx": rel_idx,
                        "price": round(close_price, 2),
                        "day": bar.get("day", ""),
                        "reason": reason,
                        "shares": take,
                        "pnl_pct": round(pnl_pct, 2),
                    })
                # 清理空仓位
                positions = [p for p in positions if p["remaining_shares"] > 0]

        # === 2. 再执行买入 (当日收盘价, 使用卖出释放的资金) ===
        if signal == "buy" and not limit_up and cash > 1000:
            # 判断档位
            tier = "标准档(≈1万)"
            target_cash = STD_POS_CASH
            vol_ratio = bs.get("vol_ratio") or 1.0
            trend = bs.get("trend") or ""
            s_type = signal_type or ""
            is_good = (
                ("买点1" in s_type or "买点3" in s_type or "回踩" in s_type or "金叉" in s_type)
                and vol_ratio > 1.2
                and trend == "多头趋势"
            )
            if is_good:
                tier = "满仓档(剩余全部)"
                target_cash = cash * 0.99  # 留1%作手续费缓冲
            shares, cost_amt, fee = _calc_shares_by_cash(close_price, cash, target_cash)
            if shares > 0 and (cost_amt + fee) <= cash:
                cash -= (cost_amt + fee)
                total_fees += fee
                pos = {
                    "entry_idx": i,
                    "entry_day": bar.get("day", ""),
                    "entry_price": round(close_price, 2),
                    "shares": shares,  # 初始股数
                    "remaining_shares": shares,
                    "style_type": style.get("style_type", "") or style_type,
                    "tp_main": style.get("tp_main"),
                    "tp_extra": style.get("tp_extra"),
                    "tp_single": style.get("tp_single"),
                    "sl_style": style.get("stop_loss"),
                    "buy_signal_type": s_type,
                    "half_done": False,
                    "entry_fee": fee,
                }
                positions.append(pos)
                rel_idx = i - start_idx
                buy_markers.append({
                    "idx": rel_idx,
                    "price": round(close_price, 2),
                    "day": bar.get("day", ""),
                    "signal_type": s_type,
                    "shares": shares,
                    "tier": tier,
                    "cost": cost_amt,
                })

        # === 记录当日权益净值 ===
        pos_mv = sum(p["remaining_shares"] * c_today for p in positions)
        equity = round(cash + pos_mv, 2)
        # 基准 buy_hold 权益
        bh_mv = bh_shares * c_today
        bh_equity = round(bh_cash + bh_mv, 2)
        equity_curve.append({
            "day": bar.get("day", ""),
            "equity": equity,
            "benchmark": bh_equity,
            "pnl_pct": round((equity / INIT_CAPITAL - 1) * 100, 2),
            "bm_pct": round((bh_equity / INIT_CAPITAL - 1) * 100, 2),
        })

    # === 最后: 强制平仓 (end_date 收盘价) ===
    # T+0.5: 当日(end_idx)买入的仓位不可当日平仓, 保留在 positions 中 (属于回测范围外的次日)
    if positions:
        last_bar = bars[end_idx]
        last_price = last_bar["close"]
        rel_idx = end_idx - start_idx
        _remaining = []
        for p in list(positions):
            if p["entry_idx"] == end_idx:
                _remaining.append(p)
                continue  # T+0.5: 回测最后一天买入, 不可当日平仓
            take = p["remaining_shares"]
            if take <= 0:
                continue
            pnl = (last_price - p["entry_price"]) * take
            pnl_pct = (last_price / p["entry_price"] - 1) * 100
            revenue = round(take * last_price, 2)
            fee = round(revenue * SELL_FEE_RATE, 2)
            cash += revenue - fee
            total_fees += fee
            hold_days = end_idx - p["entry_idx"]
            closed_trades.append({
                "entry_day": p["entry_day"],
                "exit_day": last_bar.get("day", "") + "(结)",
                "entry_price": p["entry_price"],
                "exit_price": round(last_price, 2),
                "shares": take,
                "pnl_amt": round(pnl, 2),
                "pnl_pct": round(pnl_pct, 2),
                "hold_days": hold_days,
                "fee": fee,
                "buy_signal_type": p["buy_signal_type"],
                "exit_reason": "回测截止平仓(收盘)",
                "style_type": p["style_type"],
            })
            sell_markers.append({
                "idx": rel_idx,
                "price": round(last_price, 2),
                "day": last_bar.get("day", ""),
                "reason": "回测截止(收盘)",
                "shares": take,
                "pnl_pct": round(pnl_pct, 2),
            })
        positions = _remaining

    # === 统计汇总 ===
    closed_count = len(closed_trades)
    if closed_count > 0:
        wins = [t for t in closed_trades if t["pnl_amt"] > 0]
        losses = [t for t in closed_trades if t["pnl_amt"] <= 0]
        final_equity = equity_curve[-1]["equity"] if equity_curve else INIT_CAPITAL
        total_pnl_pct = (final_equity / INIT_CAPITAL - 1) * 100
        win_rate = len(wins) / closed_count * 100 if closed_count else 0
        avg_win = (sum(t["pnl_pct"] for t in wins) / len(wins)) if wins else 0
        avg_loss = abs(sum(t["pnl_pct"] for t in losses) / len(losses)) if losses else 0
        pf = (avg_win / avg_loss) if avg_loss > 0 else float("inf") if avg_win > 0 else 0
        avg_pnl = sum(t["pnl_pct"] for t in closed_trades) / closed_count
        avg_hold = sum(t["hold_days"] for t in closed_trades) / closed_count
        total_pnl_amt = sum(t["pnl_amt"] for t in closed_trades)
        # 最大回撤: 从权益曲线
        max_dd_pct = 0.0
        peak = INIT_CAPITAL
        for pt in equity_curve:
            peak = max(peak, pt["equity"])
            dd = (pt["equity"] / peak - 1) * 100 if peak > 0 else 0
            if dd < max_dd_pct:
                max_dd_pct = dd
        max_dd_pct = abs(max_dd_pct)
        fee_pct = total_fees / INIT_CAPITAL * 100
    else:
        final_equity = INIT_CAPITAL
        total_pnl_pct = 0
        win_rate = 0; avg_win = 0; avg_loss = 0; pf = 0; avg_pnl = 0
        avg_hold = 0; total_pnl_amt = 0; max_dd_pct = 0; fee_pct = 0

    stats = {
        "initial_capital": INIT_CAPITAL,
        "final_equity": round(final_equity, 2),
        "total_pnl_pct": round(total_pnl_pct, 2),
        "total_pnl_amt": round(total_pnl_amt, 2),
        "trade_count": closed_count,
        "win_rate": round(win_rate, 1),
        "profit_factor": round(pf, 2) if isinstance(pf, float) and math.isfinite(pf) else "∞",
        "avg_pnl_pct": round(avg_pnl, 2),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "avg_hold_days": round(avg_hold, 1),
        "max_dd_pct": round(max_dd_pct, 2),
        "total_fees": round(total_fees, 2),
        "fee_ratio_pct": round(fee_pct, 3),
        "benchmark_pct": round((equity_curve[-1]["bm_pct"]), 2) if equity_curve else 0,
    }

    # bars 窗口裁剪 (给前端画图) — 包含预热段, 让 MA20/MA60 在图中完整呈现
    # 预热段: start_idx 前 PREWARM 根 (但不超过数据起点), 用 prewarm_days 标记分界
    prewarm_start = max(0, start_idx - PREWARM)
    bars_window = []
    for i in range(prewarm_start, min(end_idx + 1, n)):
        b = bars[i]
        bars_window.append({
            "day": b.get("day", ""),
            "open": round(b["open"], 2),
            "high": round(b["high"], 2),
            "low": round(b["low"], 2),
            "close": round(b["close"], 2),
            "volume": int(b.get("volume", 0)),
            "chg": round((b["close"] / (bars[i - 1]["close"] if i > 0 else b["close"]) - 1) * 100, 2),
            "is_prewarm": i < start_idx,  # True=预热段(灰色淡化), False=回测段
        })
    # 买卖点 idx 需要加上预热段偏移 (原 idx 相对 start_idx, 现在相对 prewarm_start)
    prewarm_offset = start_idx - prewarm_start
    buy_markers_out = [{**m, "idx": m["idx"] + prewarm_offset} for m in buy_markers]
    sell_markers_out = [{**m, "idx": m["idx"] + prewarm_offset} for m in sell_markers]

    return {
        "ok": True,
        "symbol": symbol,
        "start_date": bars[start_idx].get("day", ""),
        "end_date": bars[end_idx].get("day", ""),
        "window_days": len(bars_window),
        "prewarm_days": prewarm_offset,  # 预热根数 (前端用此画分界线)
        "stats": stats,
        "closed_trades": closed_trades,
        "equity_curve": equity_curve,
        "bars": bars_window,
        "buy_markers": buy_markers_out,
        "sell_markers": sell_markers_out,
    }


@app.post("/api/stock/backtest_single")
def api_stock_backtest_single(payload: dict):
    """个股分析 → 单股回测
    输入: {code, start_date:'YYYY-MM-DD'(默认3个月前), end_date:'YYYY-MM-DD'(默认今日)}
    """
    code = (payload or {}).get("code")
    if not code:
        return JSONResponse({"ok": False, "msg": "code必填"}, status_code=400)
    start_date = str((payload or {}).get("start_date", "") or "").strip()
    end_date = str((payload or {}).get("end_date", "") or "").strip()
    symbol = _to_symbol(code)
    # 拉最长 1200 天 (4年足够任何区间)
    try:
        bars = fetch_kline(symbol, datalen=1200)
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"K线拉取失败: {str(e)[:80]}"}, status_code=500)
    if len(bars) < 70:
        return JSONResponse({"ok": False, "msg": "K线不足70根"}, status_code=400)
    result = _run_single_stock_backtest(symbol, bars, start_date, end_date)
    return result


@app.on_event("startup")
def _startup():
    # 服务启动即预热缓存(默认全条件), 避免首次请求等待
    threading.Thread(target=_run_screen_thread, daemon=True).start()
    # 上试盘: 启动即开跑 + 常驻工作日 16:30 自动重扫
    threading.Thread(target=_ssp_daily_runner, daemon=True).start()
    # 磁盘缓存清理: 启动清一次 + 常驻每天09:00清一次 (20260906)
    threading.Thread(target=_cache_cleaner_loop, daemon=True).start()


# ---------- 上试盘·每日定时更新 ----------
_SSP_DAILY_HOUR = 16
_SSP_DAILY_MINUTE = 30  # A股收盘后 1.5h, 历史数据基本都齐


def _ssp_log(msg: str) -> None:
    """上试盘模块日志: 带北京时间戳打印到 stdout (uvicorn 接管日志输出)。

    20260906 修复: 该函数此前只有调用、没有定义, 每日 16:30 定时触发后必抛
    NameError; 且 except 分支里再次调用 _ssp_log, 异常直接击穿 while 循环,
    导致常驻定时线程退出 —— 之后每个交易日的自动重扫都不会再触发。
    统一在此定义, 消除未定义引用。"""
    print(f"[{bj_now()}] {msg}", flush=True)

def _is_workday(d):
    """周一=0 ~ 周五=4. 简化版(不剔除交易所休假日, 节假日少量/空量跑一次无害)"""
    return d.weekday() < 5

def _ssp_daily_runner():
    """
    常驻线程, 双机制保证每日更新:
      1) 缓存超过 TTL(30min) 首次访问会触发(见 api_ssp_screen)
      2) 工作日 16:30 准点强制重新扫描一次 (保证收盘当天最新信号入库)
      3) 外部 cron 可 POST /api/ssp-screen/run 做兜底 (见 README 说明)
    """
    import datetime as _dt
    # 一启动先等后台 init 扫完 (5s 宽松等待)
    time.sleep(5)
    last_triggered_day = None
    while True:
        try:
            now = _dt.datetime.now()
            # 工作日、到点、今天还没触发过
            if (_is_workday(now)
                and now.hour == _SSP_DAILY_HOUR
                and now.minute == _SSP_DAILY_MINUTE
                and last_triggered_day != now.date()):
                with _SSP_STATE["lock"]:
                    _SSP_STATE["scan_ts"] = 0.0
                    _SSP_STATE["data_hint"] = None
                threading.Thread(target=_run_ssp_scan_thread, daemon=True).start()
                last_triggered_day = now.date()
                _ssp_log(f"[ssp] 每日定时触发 (16:30): {now:%Y-%m-%d %H:%M}")
        except Exception as e:
            _ssp_log(f"[ssp] 每日定时线程异常: {e}")
        # 20s tick, 对小时级任务足够精确
        time.sleep(20)


# ============================================================
# 云端存储 (20260906): 持仓/标记持久化到云端数据库, 解决 Render 免费档
# 重启后 data/*.json 丢失的问题。
# 通过环境变量 DATABASE_URL 启用, 未配置时回退本地 JSON 文件(行为不变):
#   - postgres://... / postgresql://...  → Supabase / Neon 等免费 Postgres (psycopg2)
#   - sqlite:///相对或绝对路径.db        → SQLite (标准库, 也可指向托管 SQLite)
# 表结构: kv_state(key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)
# marks/positions 各存一行整包 JSON, 读写语义与原文件完全一致;
# 云端为读取优先源, 本地文件始终同步写一份作为备份镜像;
# 首次接入云端时若表为空而本地文件有数据, 自动迁移(种子上传)。
# ============================================================
import json as _json
import os as _os

_DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
_storage_backend = None   # None=未初始化; 初始化后: 'postgres' | 'sqlite' | 'file'
_STORAGE_LOG_TAG = "[storage]"


def _kv_log(msg: str) -> None:
    print(f"{_STORAGE_LOG_TAG} [{bj_now()}] {msg}", flush=True)


def _kv_dsn_kind(url: str) -> str | None:
    """识别 DATABASE_URL 类型"""
    if url.startswith(("postgres://", "postgresql://")):
        return "postgres"
    if url.startswith("sqlite:///"):
        return "sqlite"
    return None


def _kv_connect_postgres():
    """连接 Postgres (Supabase/Neon 等托管库普遍要求 SSL, 缺省补 sslmode=require)"""
    import psycopg2
    url = _DATABASE_URL
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return psycopg2.connect(url, connect_timeout=10)


def _kv_sqlite_path() -> str:
    """sqlite:/// 后面的路径; 相对路径以 app.py 所在目录为基准"""
    path = _DATABASE_URL[len("sqlite:///"):]
    if not os.path.isabs(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def _kv_connect_sqlite():
    import sqlite3
    conn = sqlite3.connect(_kv_sqlite_path(), timeout=10)
    return conn


def _kv_storage_init() -> str:
    """初始化存储后端(幂等): 建表 + 返回实际后端。失败一律回退本地文件。"""
    global _storage_backend
    if _storage_backend is not None:
        return _storage_backend
    kind = _kv_dsn_kind(_DATABASE_URL) if _DATABASE_URL else None
    if kind is None:
        if _DATABASE_URL:
            _kv_log("DATABASE_URL 无法识别(仅支持 postgres:// 或 sqlite:///), 使用本地文件")
        _storage_backend = "file"
        return _storage_backend
    try:
        ddl = ("CREATE TABLE IF NOT EXISTS kv_state ("
               "key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '')")
        if kind == "postgres":
            conn = _kv_connect_postgres()
        else:
            conn = _kv_connect_sqlite()
        try:
            cur = conn.cursor()
            cur.execute(ddl)
            conn.commit()
        finally:
            conn.close()
        _storage_backend = kind
        _kv_log(f"云端存储已启用: {kind}")
    except Exception as e:  # noqa: BLE001
        _kv_log(f"云端存储连接失败({type(e).__name__}: {e}), 回退本地文件")
        _storage_backend = "file"
    return _storage_backend


def _kv_get(key: str) -> dict | None:
    """从云端读取整包 JSON; 未启用/读失败返回 None (调用方回退本地文件)"""
    if _kv_storage_init() not in ("postgres", "sqlite"):
        return None
    try:
        if _storage_backend == "postgres":
            conn = _kv_connect_postgres()
            ph = "%s"
        else:
            conn = _kv_connect_sqlite()
            ph = "?"
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT value FROM kv_state WHERE key={ph}", (key,))
            row = cur.fetchone()
        finally:
            conn.close()
        if not row:
            return None
        data = json.loads(row[0])
        return data if isinstance(data, dict) else None
    except Exception as e:  # noqa: BLE001
        _kv_log(f"读取 {key} 失败: {e}")
        return None


def _kv_set(key: str, value: dict) -> bool:
    """整包 upsert 到云端; 返回是否成功(失败时内存数据仍在, 下次写入重试)"""
    if _kv_storage_init() not in ("postgres", "sqlite"):
        return False
    try:
        payload = json.dumps(value, ensure_ascii=False)
        now = bj_now()
        if _storage_backend == "postgres":
            conn = _kv_connect_postgres()
            sql = ("INSERT INTO kv_state(key, value, updated_at) VALUES (%s, %s, %s) "
                   "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at")
        else:
            conn = _kv_connect_sqlite()
            sql = ("INSERT INTO kv_state(key, value, updated_at) VALUES (?, ?, ?) "
                   "ON CONFLICT (key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at")
        try:
            cur = conn.cursor()
            cur.execute(sql, (key, payload, now))
            conn.commit()
        finally:
            conn.close()
        return True
    except Exception as e:  # noqa: BLE001
        _kv_log(f"写入 {key} 失败: {e}")
        return False


def _kv_read_local_file(path: str) -> dict | None:
    """读本地 JSON 文件(迁移种子/回退源), 失败返回 None"""
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = _json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _kv_write_local_file(path: str, value: dict) -> None:
    """本地 JSON 文件镜像写入 (云端模式下的备份, 文件模式下的主存储)"""
    _os.makedirs(_os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            _json.dump(value, f, ensure_ascii=False, indent=2)
    except OSError as e:
        _kv_log(f"本地文件写入失败 {path}: {e}")


def _kv_list(prefix: str) -> list[str]:
    """列出云端 kv_state 中以 prefix 开头的 key (按 key 降序 = 新→旧); 未启用云端返回 []"""
    if _kv_storage_init() not in ("postgres", "sqlite"):
        return []
    try:
        if _storage_backend == "postgres":
            conn = _kv_connect_postgres()
            ph = "%s"
        else:
            conn = _kv_connect_sqlite()
            ph = "?"
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT key FROM kv_state WHERE key LIKE {ph} ORDER BY key DESC", (prefix + "%",))
            rows = cur.fetchall()
        finally:
            conn.close()
        return [r[0] for r in rows]
    except Exception as e:  # noqa: BLE001
        _kv_log(f"列出 {prefix}* 失败: {e}")
        return []


# ============================================================
# 每日信号归档 (20260906): 选股/均线形态/上试盘结果按日存档, 供历史复盘
# 云端: kv_state key = 'signals_<YYYY-MM-DD>'; 未配云端时: cache/signal_history.json
# 每日一条记录, 三个模块各自更新自己的 section (同日重扫覆盖当日数据);
# 本地文件最多保留 _SIGNAL_KEEP_DAYS 天, 云端数据量极小暂不清理。
# ============================================================
_SIGNAL_HISTORY_FILE = _os.path.join(CACHE_DIR, "signal_history.json")
_SIGNAL_KEEP_DAYS = 120


def _archive_local_load() -> dict:
    data = _kv_read_local_file(_SIGNAL_HISTORY_FILE)
    return data if isinstance(data, dict) else {}


def _archive_local_save(d: dict) -> None:
    # 只保留最近 N 天, 防止本地文件无限膨胀
    keys = sorted(d.keys(), reverse=True)[:_SIGNAL_KEEP_DAYS]
    _kv_write_local_file(_SIGNAL_HISTORY_FILE, {k: d[k] for k in keys})


def _archive_get(date: str) -> dict | None:
    if _kv_storage_init() in ("postgres", "sqlite"):
        cloud = _kv_get(f"signals_{date}")
        if cloud is not None:
            return cloud
    local = _archive_local_load()
    return local.get(date)


def _archive_put(date: str, section: str, payload: dict) -> None:
    """合并写入某日某模块的信号结果 (cloud + 本地镜像双写)"""
    rec = _archive_get(date) or {}
    rec[section] = payload
    rec["date"] = date
    rec["updated_at"] = bj_now()
    if _kv_storage_init() in ("postgres", "sqlite"):
        if not _kv_set(f"signals_{date}", rec):
            _kv_log(f"信号归档上云失败({date}/{section}), 仅保留本地镜像")
    local = _archive_local_load()
    local[date] = rec
    _archive_local_save(local)


def _archive_dates() -> list[str]:
    """已归档日期列表(新→旧): 云端 keys + 本地 keys 合并去重"""
    dates = set()
    for k in _kv_list("signals_"):
        if k.startswith("signals_") and len(k) == len("signals_2026-09-06"):
            dates.add(k[len("signals_"):])
    dates.update(_archive_local_load().keys())
    return sorted(dates, reverse=True)[:_SIGNAL_KEEP_DAYS]


# ============================================================
# 股票标记模块 (3 级留意 + 移除关注 + 标注时间)
# ============================================================
import json as _json
import os as _os

_MARKS_DIR = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "data")
_MARKS_FILE = _os.path.join(_MARKS_DIR, "stock_marks.json")
_MARKS_LOCK = threading.Lock()

# mark 字符说明:
#   '"' (双引号) -> 白色留意
#   '*' (星号)   -> 黄色关注
#   '!' (叹号)   -> 红色特别关注
#   None          -> 未标记
# removed=True   -> 删除线(移除关注)
_MARKS: dict[str, dict] = {}

def _load_marks():
    """加载标记: 云端优先; 云端无数据而本地文件有 → 自动迁移上云; 全失败用空表 (20260906)"""
    global _MARKS
    if _kv_storage_init() in ("postgres", "sqlite"):
        cloud = _kv_get("marks")
        if cloud is not None:
            _MARKS = cloud
            return
        seed = _kv_read_local_file(_MARKS_FILE)
        if seed is not None:
            _MARKS = seed
            if _kv_set("marks", seed):
                _kv_log(f"标记数据已从本地文件迁移上云 ({len(seed)} 条)")
            return
    if not _os.path.isfile(_MARKS_FILE):
        _MARKS = {}
        return
    try:
        with open(_MARKS_FILE, "r", encoding="utf-8") as f:
            data = _json.load(f)
        if isinstance(data, dict):
            _MARKS = data
    except Exception:
        _MARKS = {}

def _save_marks():
    """保存标记: 云端模式=云端 upsert + 本地文件镜像备份; 文件模式=仅本地 (20260906)"""
    if _storage_backend in ("postgres", "sqlite"):
        _kv_set("marks", _MARKS)
    _kv_write_local_file(_MARKS_FILE, _MARKS)

_load_marks()

def _mark_meta(code, name=None):
    """获取某只股票的标记元信息 (拷贝)"""
    m = _MARKS.get(code)
    if not m:
        return {"code": code, "name": name or "", "mark": None, "removed": False,
                "created_at": None, "updated_at": None, "note": "", "history": []}
    return dict(m)

@app.get("/api/marks")
def api_marks_all():
    """获取所有股票标记"""
    with _MARKS_LOCK:
        return JSONResponse({"marks": {k: dict(v) for k, v in _MARKS.items()}})

@app.post("/api/marks")
def api_mark_set(payload: dict):
    """设置/更新某只股票的标记.
    body: {code, name?, mark?, removed?, note?}
    mark 可取值: '"' | '*' | '!' | null (传 null 清除标记等级, 但保留记录)
    """
    code = str(payload.get("code", "")).strip()
    if not code:
        return JSONResponse({"ok": False, "msg": "code 不能为空"}, status_code=400)
    name = payload.get("name") or ""
    mark = payload.get("mark", None)
    removed = payload.get("removed")
    note = payload.get("note")
    with _MARKS_LOCK:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        cur = _MARKS.get(code)
        if not cur:
            cur = {"code": code, "name": name, "mark": None, "removed": False,
                   "created_at": now, "updated_at": now, "note": "", "history": []}
        if name and not cur.get("name"):
            cur["name"] = name
        if mark is not None:
            if mark not in ('"', "*", "!"):
                return JSONResponse({"ok": False, "msg": "mark 只能是 '\"' / '*' / '!' 中的一个"}, status_code=400)
            cur["mark"] = mark
            # 设置标记等级时同时取消忽略状态
            was_removed = cur.get("removed")
            cur["removed"] = False
            if was_removed:
                cur["history"] = (cur.get("history") or []) + [
                    {"time": now, "action": f"设置标记为 {mark} (取消忽略)"}]
            else:
                cur["history"] = (cur.get("history") or []) + [
                    {"time": now, "action": f"设置标记为 {mark}"}]
        if removed is not None:
            cur["removed"] = bool(removed)
            if cur["removed"]:
                # 忽略时同时清除标记等级
                old_mark = cur.get("mark")
                cur["mark"] = None
                if old_mark:
                    cur["history"] = (cur.get("history") or []) + [
                        {"time": now, "action": f"忽略此股 (清除原标记 {old_mark})"}]
                else:
                    cur["history"] = (cur.get("history") or []) + [
                        {"time": now, "action": "忽略此股"}]
            else:
                cur["history"] = (cur.get("history") or []) + [
                    {"time": now, "action": "取消忽略此股"}]
        if note is not None:
            cur["note"] = note
        cur["updated_at"] = now
        # 既无标记等级也未删除 -> 整条记录可保留(留痕) 也可清除. 这里保留记录以便回看历史
        _MARKS[code] = cur
        _save_marks()
        return JSONResponse({"ok": True, "mark": dict(cur)})

@app.delete("/api/marks/{code}")
def api_mark_delete(code: str):
    """彻底删除某只股票的标记记录 (不留痕)"""
    code = str(code).strip()
    with _MARKS_LOCK:
        if code in _MARKS:
            del _MARKS[code]
            _save_marks()
            return JSONResponse({"ok": True})
        return JSONResponse({"ok": False, "msg": "该股票无标记记录"}, status_code=404)

@app.get("/api/marks/list")
def api_marks_list(mark: str = None, removed: bool = None):
    """按条件列出标记股票.
    mark 可传 key 名 (quote/star/bang) 或字符 ('"/!/*)  — 推荐用 key 名, 避免代理对特殊字符编码解析失败
    removed=true  仅看已移除
    不传参数则返回全部
    返回的列表附带当前价格/涨跌幅 (从 spot 取)
    """
    _MARK_KEY_TO_CHAR = {"quote": '"', "star": "*", "bang": "!"}
    with _MARKS_LOCK:
        items = [dict(v) for v in _MARKS.values()]
    if mark is not None:
        # 兼容 key 名与字符两种传法
        mark_char = _MARK_KEY_TO_CHAR.get(mark, mark)
        items = [x for x in items if x.get("mark") == mark_char]
    if removed is not None:
        items = [x for x in items if bool(x.get("removed")) == bool(removed)]
    elif mark is not None:
        # 按标记等级筛选时, 默认排除已忽略(removed)的股票, 与标签计数口径一致
        items = [x for x in items if not bool(x.get("removed"))]
    # 附带实时行情
    try:
        spot = fetch_spot_all()
        spot_map = {r["code"]: r for r in spot}
    except Exception:
        spot_map = {}
    out = []
    for x in items:
        s = spot_map.get(x["code"], {})
        try:
            amt = float(s.get("amount", 0) or 0)
        except (ValueError, TypeError):
            amt = 0.0
        out.append({
            **x,
            "price": round(float(s.get("trade", 0) or 0), 2) if s.get("trade") else None,
            "change_pct": round(float(s.get("changepercent", 0) or 0), 2) if s.get("changepercent") is not None else None,
            "amount_yi": round(amt / 1e8, 2) if amt else None,
            "industry": get_industry(x["code"]),
        })
    return JSONResponse({"marks": out, "count": len(out)})

# ============================================================
# 持仓管理 (买卖点标记)
# ============================================================
_POSITIONS_FILE = _os.path.join(_MARKS_DIR, "positions.json")
_POSITIONS_LOCK = threading.Lock()
_POSITIONS: dict[str, dict] = {}
_OP_ID = 0  # 操作记录自增ID
_UNDO_BUFFER: list[dict] = []  # 撤销缓冲区: 存放最近被删除的操作 (含 code + 被删操作 + 删除前持仓快照)
_UNDO_MAX = 50  # 最多保留 50 条撤销记录

def _load_positions():
    """加载持仓: 云端优先; 云端无数据而本地文件有 → 自动迁移上云; 全失败用空表 (20260906)"""
    global _POSITIONS, _OP_ID
    data = None
    if _kv_storage_init() in ("postgres", "sqlite"):
        data = _kv_get("positions")
        if data is None:
            seed = _kv_read_local_file(_POSITIONS_FILE)
            if seed is not None:
                data = seed
                if _kv_set("positions", seed):
                    _kv_log(f"持仓数据已从本地文件迁移上云 ({len(seed)} 只)")
    if data is None:
        if not _os.path.isfile(_POSITIONS_FILE):
            _POSITIONS = {}
            return
        try:
            with open(_POSITIONS_FILE, "r", encoding="utf-8") as f:
                data = _json.load(f)
        except Exception:
            _POSITIONS = {}
            return
    if isinstance(data, dict):
        _POSITIONS = data
        # 重建最大操作ID
        for p in _POSITIONS.values():
            for op in p.get("operations", []):
                if op.get("id", 0) > _OP_ID:
                    _OP_ID = op["id"]

def _save_positions():
    """保存持仓: 云端模式=云端 upsert + 本地文件镜像备份; 文件模式=仅本地 (20260906)"""
    if _storage_backend in ("postgres", "sqlite"):
        _kv_set("positions", _POSITIONS)
    _kv_write_local_file(_POSITIONS_FILE, _POSITIONS)

_load_positions()

def _limit_pct(code: str, name: str = "") -> float:
    """按板块返回涨跌停幅度 (如 0.1 / 0.2 / 0.05)"""
    code = str(code)
    name = name or ""
    if "ST" in name.upper() or "*ST" in name.upper():
        return 0.05
    if code.startswith(("300", "301", "688")):
        return 0.20
    if code.startswith(("8", "4")):  # 北交所
        return 0.30
    return 0.10

# 20260906 修复: 原名 _is_limit_up/_is_limit_down 与回测引擎的 (bar, prev_close) 版本
# (约5607行) 重名, 且定义在其后, 全局遮蔽了回测版本 —— 单股回测调用时直接抛
# TypeError(_is_limit_up() missing 1 required positional argument: 'change_pct')。
# 此处两个函数仅用于持仓买卖时基于实时涨跌幅的涨跌停判断, 改名以消除遮蔽。
def _spot_is_limit_up(code: str, name: str, change_pct: float) -> bool:
    return change_pct >= _limit_pct(code, name) * 100 - 0.1

def _spot_is_limit_down(code: str, name: str, change_pct: float) -> bool:
    return change_pct <= -_limit_pct(code, name) * 100 + 0.1

def _next_op_id():
    global _OP_ID
    _OP_ID += 1
    return _OP_ID

def _ensure_position(code: str, name: str = "") -> dict:
    cur = _POSITIONS.get(code)
    if not cur:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        cur = {
            "code": code, "name": name or "",
            "lots": 0, "avg_cost": 0.0, "total_cost": 0.0,
            "operations": [], "realized_pnl": 0.0,
            "is_closed": False,
            "created_at": now, "updated_at": now,
        }
        _POSITIONS[code] = cur
    elif name and not cur.get("name"):
        cur["name"] = name
    return cur

def _recompute_position(code: str):
    """根据剩余操作记录按时间顺序重算持仓汇总: lots/total_cost/avg_cost/realized_pnl/is_closed"""
    cur = _POSITIONS.get(code)
    if not cur:
        return
    ops = sorted(cur.get("operations", []), key=lambda o: o.get("time", ""))
    lots = 0
    total_cost = 0.0
    avg_cost = 0.0
    realized_pnl = 0.0
    for op in ops:
        if op.get("type") == "buy":
            bl = int(op.get("lots", 0))
            amt = float(op.get("amount", 0.0))
            lots += bl
            total_cost += amt
            avg_cost = round(total_cost / (lots * 100), 4) if lots > 0 else 0.0
        elif op.get("type") == "sell":
            sl = int(op.get("lots", 0))
            px = float(op.get("price", 0.0))
            realized = round((px - avg_cost) * sl * 100, 2)
            new_lots = lots - sl
            if new_lots > 0:
                total_cost = round(total_cost * new_lots / lots, 2) if lots > 0 else 0.0
                lots = new_lots
            else:
                lots = 0
                total_cost = 0.0
                avg_cost = 0.0
            realized_pnl = round(realized_pnl + realized, 2)
    cur["lots"] = lots
    cur["total_cost"] = round(total_cost, 2)
    cur["avg_cost"] = avg_cost
    cur["realized_pnl"] = realized_pnl
    cur["is_closed"] = (lots == 0 and len(ops) > 0)

@app.get("/api/positions")
def api_positions_all():
    """获取所有持仓记录 (含已清仓)"""
    with _POSITIONS_LOCK:
        return JSONResponse({"positions": {k: dict(v) for k, v in _POSITIONS.items()}})

@app.post("/api/position/buy")
def api_position_buy(payload: dict):
    """买入: {code, name, lots, price?, change_pct?}
    lots 为手数 (1手=100股); price 为当日收盘价(若前端未传或<=0, 后端取当前spot价); change_pct 用于判断涨停
    """
    code = str(payload.get("code", "")).strip()
    if not code:
        return JSONResponse({"ok": False, "msg": "code 不能为空"}, status_code=400)
    name = payload.get("name") or ""
    try:
        lots = int(payload.get("lots", 0))
    except (ValueError, TypeError):
        return JSONResponse({"ok": False, "msg": "lots 参数错误"}, status_code=400)
    if lots <= 0:
        return JSONResponse({"ok": False, "msg": "买入手数必须大于0"}, status_code=400)
    # 价格: 前端传则用之, 否则取当前spot价
    try:
        price = float(payload.get("price", 0))
    except (ValueError, TypeError):
        price = 0.0
    change_pct = payload.get("change_pct")
    try:
        change_pct = float(change_pct) if change_pct is not None else None
    except (ValueError, TypeError):
        change_pct = None
    if price <= 0 or change_pct is None:
        # 从spot补齐价格和涨跌幅
        try:
            spot = fetch_spot_all()
            s = next((r for r in spot if r.get("code") == code), None)
            if s:
                if price <= 0:
                    price = round(float(s.get("trade", 0) or 0), 2)
                if change_pct is None and s.get("changepercent") is not None:
                    change_pct = round(float(s.get("changepercent")), 2)
        except Exception:
            pass
    if price <= 0:
        return JSONResponse({"ok": False, "msg": "无法获取当前价格, 请稍后重试"}, status_code=400)
    if change_pct is not None and _spot_is_limit_up(code, name, change_pct):
        return JSONResponse({"ok": False, "msg": "涨停板不可买入"}, status_code=400)
    with _POSITIONS_LOCK:
        cur = _ensure_position(code, name)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        amount = round(price * lots * 100, 2)
        new_lots = cur["lots"] + lots
        new_total = cur["total_cost"] + amount
        new_avg = round(new_total / (new_lots * 100), 4) if new_lots > 0 else 0.0
        cur["lots"] = new_lots
        cur["total_cost"] = round(new_total, 2)
        cur["avg_cost"] = new_avg
        cur["is_closed"] = False
        cur["operations"].append({
            "id": _next_op_id(), "type": "buy",
            "lots": lots, "price": price, "amount": amount,
            "date": now[:10], "time": now,
        })
        cur["updated_at"] = now
        if name:
            cur["name"] = name
        _save_positions()
        pos = dict(cur)
        pos["price"] = price  # 附带当前价, 供前端缓存
        return JSONResponse({"ok": True, "position": pos})

@app.post("/api/position/sell")
def api_position_sell(payload: dict):
    """卖出: {code, lots, price?, change_pct?}
    卖出手数不可超过当前持仓; 跌停不可卖; 若前端未传价则取spot价
    """
    code = str(payload.get("code", "")).strip()
    if not code:
        return JSONResponse({"ok": False, "msg": "code 不能为空"}, status_code=400)
    try:
        lots = int(payload.get("lots", 0))
    except (ValueError, TypeError):
        return JSONResponse({"ok": False, "msg": "lots 参数错误"}, status_code=400)
    if lots <= 0:
        return JSONResponse({"ok": False, "msg": "卖出手数必须大于0"}, status_code=400)
    try:
        price = float(payload.get("price", 0))
    except (ValueError, TypeError):
        price = 0.0
    name = payload.get("name") or ""
    change_pct = payload.get("change_pct")
    try:
        change_pct = float(change_pct) if change_pct is not None else None
    except (ValueError, TypeError):
        change_pct = None
    if price <= 0 or change_pct is None:
        try:
            spot = fetch_spot_all()
            s = next((r for r in spot if r.get("code") == code), None)
            if s:
                if price <= 0:
                    price = round(float(s.get("trade", 0) or 0), 2)
                if change_pct is None and s.get("changepercent") is not None:
                    change_pct = round(float(s.get("changepercent")), 2)
        except Exception:
            pass
    if price <= 0:
        return JSONResponse({"ok": False, "msg": "无法获取当前价格, 请稍后重试"}, status_code=400)
    if change_pct is not None and _spot_is_limit_down(code, name, change_pct):
        return JSONResponse({"ok": False, "msg": "跌停板不可卖出"}, status_code=400)
    with _POSITIONS_LOCK:
        cur = _POSITIONS.get(code)
        if not cur or cur["lots"] <= 0:
            return JSONResponse({"ok": False, "msg": "该股票当前无持仓"}, status_code=400)
        if lots > cur["lots"]:
            return JSONResponse({"ok": False, "msg": f"卖出手数({lots})超过当前持仓({cur['lots']}手)"}, status_code=400)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        amount = round(price * lots * 100, 2)
        # 已实现盈亏 = (卖出价 - 平均成本) * 卖出股数
        realized = round((price - cur["avg_cost"]) * lots * 100, 2)
        new_lots = cur["lots"] - lots
        if new_lots > 0:
            # 部分卖出: 总成本按比例减少
            cur["total_cost"] = round(cur["total_cost"] * new_lots / cur["lots"], 2)
            cur["lots"] = new_lots
            # avg_cost 不变
        else:
            # 全部卖出: 清仓
            cur["lots"] = 0
            cur["total_cost"] = 0.0
            cur["avg_cost"] = 0.0
            cur["is_closed"] = True
        cur["realized_pnl"] = round(cur.get("realized_pnl", 0.0) + realized, 2)
        cur["operations"].append({
            "id": _next_op_id(), "type": "sell",
            "lots": lots, "price": price, "amount": amount,
            "realized_pnl": realized,
            "date": now[:10], "time": now,
        })
        cur["updated_at"] = now
        if name and not cur.get("name"):
            cur["name"] = name
        _save_positions()
        pos = dict(cur)
        pos["price"] = price  # 附带当前价, 供前端缓存
        return JSONResponse({"ok": True, "position": pos, "realized_pnl": realized})

def _enrich_positions(items, with_spot=True):
    """为持仓列表附带实时行情: 当前价、收益率、持仓市值"""
    if not items:
        return items
    spot_map = {}
    if with_spot:
        try:
            spot = fetch_spot_all()
            spot_map = {r["code"]: r for r in spot}
        except Exception:
            spot_map = {}
    out = []
    for x in items:
        s = spot_map.get(x["code"], {})
        cur_price = float(s.get("trade", 0) or 0) if s.get("trade") else None
        lots = x.get("lots", 0)
        avg_cost = x.get("avg_cost", 0.0)
        market_value = round(cur_price * lots * 100, 2) if (cur_price and lots > 0) else 0.0
        cost_value = round(avg_cost * lots * 100, 2) if lots > 0 else 0.0
        pnl = round(market_value - cost_value, 2) if lots > 0 else 0.0
        pnl_pct = round((cur_price - avg_cost) / avg_cost * 100, 2) if (avg_cost and lots > 0 and cur_price) else None
        out.append({
            **x,
            "price": round(cur_price, 2) if cur_price else None,
            "change_pct": round(float(s.get("changepercent", 0) or 0), 2) if s.get("changepercent") is not None else None,
            "market_value": market_value,
            "cost_value": cost_value,
            "unrealized_pnl": pnl,
            "pnl_pct": pnl_pct,
            "industry": get_industry(x["code"]),
        })
    return out

@app.get("/api/positions/holding")
def api_positions_holding():
    """当前持仓 (lots>0)"""
    with _POSITIONS_LOCK:
        items = [dict(v) for v in _POSITIONS.values() if v.get("lots", 0) > 0]
    items = _enrich_positions(items)
    items.sort(key=lambda x: -x.get("market_value", 0))
    return JSONResponse({"positions": items, "count": len(items)})

@app.get("/api/positions/closed")
def api_positions_closed():
    """已清仓 (lots=0 且有操作记录)"""
    with _POSITIONS_LOCK:
        items = [dict(v) for v in _POSITIONS.values()
                 if v.get("lots", 0) == 0 and v.get("operations")]
    items = _enrich_positions(items, with_spot=True)
    # 已清仓按已实现盈亏排序
    items.sort(key=lambda x: -x.get("realized_pnl", 0))
    return JSONResponse({"positions": items, "count": len(items)})

@app.delete("/api/position/{code}")
def api_position_delete(code: str):
    """彻底删除某只股票的持仓记录 (不留痕)"""
    code = str(code).strip()
    with _POSITIONS_LOCK:
        if code in _POSITIONS:
            del _POSITIONS[code]
            _save_positions()
            return JSONResponse({"ok": True})
        return JSONResponse({"ok": False, "msg": "该股票无持仓记录"}, status_code=404)

@app.get("/api/positions/today")
def api_positions_today():
    """获取今日所有买卖操作 (跨股票, 按时间倒序)"""
    today = time.strftime("%Y-%m-%d")
    with _POSITIONS_LOCK:
        out = []
        for code, p in _POSITIONS.items():
            for op in p.get("operations", []):
                if op.get("date") == today:
                    out.append({
                        "code": code,
                        "name": p.get("name", ""),
                        **op,
                    })
    out.sort(key=lambda x: x.get("time", ""), reverse=True)
    return JSONResponse({"operations": out, "count": len(out)})

@app.get("/api/kline")
def api_kline(code: str = "", datalen: int = 122):
    """轻量K线接口 (20260906): 供前端悬浮弹框预览个股近半年日K(约122个交易日)。
    与 /api/stock/analyze 不同, 不做任何指标计算, 只回K线, 开销极小。"""
    code = (code or "").strip()
    if not code:
        return JSONResponse({"error": "code is required"}, status_code=400)
    datalen = max(10, min(600, datalen))
    symbol = _to_symbol(code)
    bars = fetch_kline(symbol, datalen=datalen)
    if not bars:
        return JSONResponse({"error": f"无K线数据 {code}"}, status_code=404)
    # 缓存可能比请求的更长(如回测拉过1200根), 按请求裁剪尾部
    bars = bars[-datalen:]
    chgs = daily_changes(bars)
    out = [{"day": b["day"], "open": b["open"], "high": b["high"], "low": b["low"],
            "close": b["close"], "volume": b["volume"],
            "chg": round(c, 2) if not math.isnan(c) else 0}
           for b, c in zip(bars, chgs)]
    # 名称获取 (20260906 修复浮窗只显示代码没有名称):
    # 1) 搜索索引; 2) 股票池快照universe_latest.json(每次成功拉取行情后更新,
    #    服务重启后也立即可用)。均为本地读取, 不触发额外拉取。
    name = ""
    for it in _stock_search_cache.get("items", []):
        if it.get("code") == code or it.get("symbol") == symbol:
            name = it.get("name", "")
            break
    if not name:
        # 注意: universe_latest.json 是list类型, 不能用只认dict的_kv_read_local_file
        try:
            if os.path.isfile(_UNIVERSE_FILE):
                with open(_UNIVERSE_FILE, "r", encoding="utf-8") as f:
                    uni = json.load(f)
                if isinstance(uni, list):
                    for it in uni:
                        if it.get("code") == code or it.get("symbol") == symbol:
                            name = it.get("name", "")
                            break
        except Exception:  # noqa: BLE001
            pass
    return {"code": code, "symbol": symbol, "name": name, "bars": out}


@app.get("/api/spot/{code}")
def api_spot_single(code: str):
    """获取单只股票的实时价格 (用于交易菜单价格兜底)"""
    code = str(code).strip()
    try:
        spot = fetch_spot_all()
        s = next((r for r in spot if r.get("code") == code), None)
        if s and s.get("trade"):
            price = round(float(s.get("trade", 0)), 2)
            change_pct = round(float(s.get("changepercent", 0)), 2) if s.get("changepercent") is not None else None
            return JSONResponse({"ok": True, "code": code, "price": price, "change_pct": change_pct})
    except Exception:
        pass
    return JSONResponse({"ok": False, "msg": "无法获取实时价格"}, status_code=404)

@app.delete("/api/position/{code}/operation/{op_id}")
def api_position_operation_delete(code: str, op_id: int):
    """删除某只股票的某条买卖操作记录, 删除后按剩余操作重算持仓汇总
    被删操作会存入撤销缓冲区, 可通过 POST /api/position/undo 恢复 (限当天操作)
    """
    global _UNDO_BUFFER
    code = str(code).strip()
    with _POSITIONS_LOCK:
        cur = _POSITIONS.get(code)
        if not cur:
            return JSONResponse({"ok": False, "msg": "该股票无持仓记录"}, status_code=404)
        ops = cur.get("operations", [])
        idx = next((i for i, o in enumerate(ops) if o.get("id") == op_id), None)
        if idx is None:
            return JSONResponse({"ok": False, "msg": "操作记录不存在"}, status_code=404)
        removed = ops.pop(idx)
        # 保存删除前的持仓快照 (用于撤销时恢复)
        before_snapshot = {
            "lots": cur.get("lots", 0),
            "total_cost": cur.get("total_cost", 0.0),
            "avg_cost": cur.get("avg_cost", 0.0),
            "realized_pnl": cur.get("realized_pnl", 0.0),
            "is_closed": cur.get("is_closed", False),
        }
        was_empty = not ops
        if was_empty:
            del _POSITIONS[code]
        else:
            _recompute_position(code)
            cur["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        # 存入撤销缓冲区 (最新的放前面)
        _UNDO_BUFFER.insert(0, {
            "code": code,
            "name": cur.get("name", ""),
            "operation": removed,
            "before": before_snapshot,
            "deleted_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        if len(_UNDO_BUFFER) > _UNDO_MAX:
            _UNDO_BUFFER = _UNDO_BUFFER[:_UNDO_MAX]
        _save_positions()
        return JSONResponse({
            "ok": True,
            "position": dict(cur) if code in _POSITIONS else None,
            "removed": removed,
            "can_undo": True,
        })

@app.post("/api/position/undo")
def api_position_undo():
    """撤销最近一次删除的买卖操作: 从撤销缓冲区取出被删操作, 重新插回持仓并重算
    仅允许撤销当天的操作删除 (缓冲区条目需为今天)
    """
    global _UNDO_BUFFER
    today = time.strftime("%Y-%m-%d")
    with _POSITIONS_LOCK:
        if not _UNDO_BUFFER:
            return JSONResponse({"ok": False, "msg": "没有可撤销的操作"}, status_code=404)
        entry = _UNDO_BUFFER.pop(0)
        code = entry["code"]
        op = entry["operation"]
        # 安全检查: 仅允许撤销当天的操作
        if op.get("date") != today:
            _UNDO_BUFFER.insert(0, entry)  # 放回去
            return JSONResponse({"ok": False, "msg": "仅可撤销当天的操作"}, status_code=400)
        cur = _POSITIONS.get(code)
        if cur is None:
            # 持仓已被清空, 重新创建
            cur = _ensure_position(code, entry.get("name", ""))
            cur["operations"] = []
        # 检查该操作是否已存在 (防止重复撤销)
        if any(o.get("id") == op.get("id") for o in cur.get("operations", [])):
            return JSONResponse({"ok": False, "msg": "该操作已存在, 无需撤销"}, status_code=400)
        cur.setdefault("operations", []).append(dict(op))
        _recompute_position(code)
        cur["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        _save_positions()
        return JSONResponse({
            "ok": True,
            "position": dict(cur),
            "restored": op,
        })

# 静态前端 (禁用缓存, 确保更新即时生效)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    r = FileResponse("static/index.html")
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r


if __name__ == "__main__":
    import uvicorn
    # 云平台(Render/Railway)通过 PORT 环境变量指定端口, 默认 8000
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
