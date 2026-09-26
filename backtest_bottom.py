# -*- coding: utf-8 -*-
"""日地量出底后上涨概率回测 (20260927)
对比 baseline 与组合增强条件: 站上MA10 / MACD金叉 / 不创新低 / 大盘过滤
统计"出地量日后 N 个交易日上涨概率 + 平均收益"。
复用 app.py 的 fetch_kline 拉数据。
"""
import json
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import app

N_AFTER = 5          # 出底后看 N 个交易日
LOOK = 122           # 价区间窗口
W = 60               # 量基准窗口
SAMPLE = 600         # 抽样股票数（全市场太大, 抽样代表）
DALEN = 250          # 每只拉250个交易日(~1年)


def single_bottom(bars, i):
    """bars 第 i 根是否满足单日'底量低价'（量+价+A/B，与后端一致）。返回bool"""
    if i <= W:
        return False
    vol = float(bars[i]["volume"] or 0)
    if vol <= 0:
        return False
    prev = bars[i - W:i]
    vmin = min(float(b["volume"]) for b in prev)
    vavg = sum(float(b["volume"]) for b in prev) / W
    if vavg <= 0 or not (vmin > 0):
        return False
    vol_ok = (vol <= vmin * 1.15) or (vol <= vavg * 0.6)
    if not vol_ok:
        return False
    st = max(0, i - LOOK)
    win = bars[st:i + 1]
    p_hi = max(float(b["high"]) for b in win)
    p_lo = min(float(b["low"]) for b in win)
    if p_hi <= p_lo:
        return False
    pos = (float(bars[i]["close"]) - p_lo) / (p_hi - p_lo)
    if pos > 0.18:
        return False
    steady = False
    if i >= 30:
        cs = [float(b["close"]) for b in bars[i - 9:i + 1]]
        lp = [float(b["low"]) for b in bars[i - 29:i - 9]]
        steady = not (min(cs) < min(lp))
    shrink = False
    v10 = sum(float(b["volume"]) for b in bars[i - 9:i + 1]) / 10
    v60 = sum(float(b["volume"]) for b in bars[i - 59:i + 1]) / 60
    if v60 > 0:
        shrink = (v10 <= v60 * 0.75)
    return steady or shrink


def sma(arr, p):
    out = [None] * len(arr)
    s = 0.0
    for i, v in enumerate(arr):
        s += v
        if i >= p:
            s -= arr[i - p]
        if i >= p - 1:
            out[i] = s / p
    return out


def macd(closes):
    """返回 EMA12, EMA26, DIF, DEA 列表（DIF=ema12-ema26, DEA=DIF的ema9,SMA）。"""
    def ema(arr, n):
        k = 2 / (n + 1)
        e = []
        prev = None
        for v in arr:
            prev = v if prev is None else v * k + prev * (1 - k)
            e.append(prev)
        return e
    e12 = ema(closes, 12)
    e26 = ema(closes, 26)
    dif = [a - b for a, b in zip(e12, e26)]
    dea = ema(dif, 9)
    return dif, dea


def index_ma20_ok():
    """大盘过滤: 上证指数(000001)收盘>=MA20。失败则放宽为True(不阻挡)。"""
    try:
        bars = app.fetch_kline("sh000001", datalen=60)
        if not bars or len(bars) < 20:
            return True
        c = [float(b["close"]) for b in bars]
        last = c[-1]
        ma20 = sum(c[-20:]) / 20
        return last >= ma20
    except Exception:
        return True


def analyze_stock(code):
    """对单只股票收集出底事件在各条件下的5日表现。返回统计字典。"""
    symbol = app._to_symbol(code)
    try:
        bars = app.fetch_kline(symbol, datalen=DALEN)
    except Exception:
        return None
    if not bars or len(bars) < W + 10:
        return None
    n = len(bars)
    closes = [float(b["close"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    ma10 = sma(closes, 10)
    dif, dea = macd(closes)
    stats = {
        "base": [], "ma10": [], "macd": [], "nlow": [],
        "ma10_macd": [], "ma10_nlow": [], "macd_nlow": [], "all": []
    }
    start = max(W, 61)
    for i in range(start, n - N_AFTER):
        if not single_bottom(bars, i):
            continue
        ret = (closes[i + N_AFTER] / closes[i] - 1) * 100  # 5日后收益%
        stats["base"].append(ret)
        # C1: 站上MA10
        if ma10[i] and closes[i] >= ma10[i]:
            stats["ma10"].append(ret)
        # C2: MACD低位金叉(出底当日 dif>dea且前日<=, 且dif<0低位)
        prev_ok = i >= 1 and dif[i - 1] <= dea[i - 1]
        if dif[i] > dea[i] and prev_ok and dif[i] < 0:
            stats["macd"].append(ret)
        # C3: 不再创新低(近5日最低 > 前20日最低)
        if i >= 30:
            recent = min(lows[i - 4:i + 1])
            prior = min(lows[i - 29:i - 4])
            if recent > prior:
                stats["nlow"].append(ret)
        # 组合
        c1 = ma10[i] is not None and closes[i] >= ma10[i]
        c2 = dif[i] > dea[i] and (i >= 1 and dif[i - 1] <= dea[i - 1]) and dif[i] < 0
        c3 = False
        if i >= 30:
            recent = min(lows[i - 4:i + 1])
            prior = min(lows[i - 29:i - 4])
            c3 = recent > prior
        if c1 and c2:
            stats["ma10_macd"].append(ret)
        if c1 and c3:
            stats["ma10_nlow"].append(ret)
        if c2 and c3:
            stats["macd_nlow"].append(ret)
        if c1 and c2 and c3:
            stats["all"].append(ret)
    return stats


def summarize(name, rets):
    if not rets:
        return f"{name:<18} n=0"
    up = sum(1 for r in rets if r > 0) / len(rets) * 100
    avg = sum(rets) / len(rets)
    med = sorted(rets)[len(rets) // 2]
    return f"{name:<18} n={len(rets):>5}  涨概率={up:5.1f}%  均收益={avg:+.2f}%  中位={med:+.2f}%"


def main():
    # 股票池: 采用市场快照, 抽样
    pool = []
    try:
        rows = app.fetch_spot_all()
        pool = [str(r.get("code") or "") for r in rows if r.get("code")]
    except Exception as e:
        print("spot fail", e)
    if pool:
        random.seed(20260927)
        pool = random.sample(pool, min(SAMPLE, len(pool)))
    else:
        # 兜底用一批常见代码
        pool = ["600519", "000858", "300750", "002594", "601899", "000568"]
    print(f"抽样 {len(pool)} 只, 每只后 {N_AFTER} 日统计……")
    agg = {k: [] for k in
           ("base", "ma10", "macd", "nlow", "ma10_macd", "ma10_nlow", "macd_nlow", "all")}
    done = 0
    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(analyze_stock, c): c for c in pool}
        for f in as_completed(futs):
            try:
                st = f.result()
            except Exception:
                st = None
            if st:
                for k in agg:
                    agg[k].extend(st.get(k, []))
            done += 1
            if done % 100 == 0:
                print(f"  ... {done}/{len(pool)}")
    print("\n============ 出地量后 5 日表现 ============")
    print(summarize("Base 仅当前口径", agg["base"]))
    print(summarize("+站上MA10", agg["ma10"]))
    print(summarize("+MACD低位金叉", agg["macd"]))
    print(summarize("+不再创新低", agg["nlow"]))
    print(summarize("+MA10且MACD金叉", agg["ma10_macd"]))
    print(summarize("+MA10且不创新低", agg["ma10_nlow"]))
    print(summarize("+MACD且不创新低", agg["macd_nlow"]))
    print(summarize("+三者全满足", agg["all"]))
    # 大盘过滤对该批次整体影响
    idx_ok = index_ma20_ok()
    print(f"\n大盘(上证>=MA20)={idx_ok}")
    if not idx_ok:
        print("  → 若加大盘过滤, 本批次全部剔除")


if __name__ == "__main__":
    main()