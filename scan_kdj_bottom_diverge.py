#!/usr/bin/env python3
"""扫描全市场 KDJ 底背离的股票 (多线程, 复用 app.py 的 calc_kdj_system)"""
import sys, os, time, math
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app
from app import fetch_spot_all, fetch_kline, calc_kdj, calc_kdj_system, calc_vp_system, analyze_buy_sell

def scan_one(r):
    code = str(r.get("code", ""))
    symbol = str(r.get("symbol") or "")
    name = r.get("name", "")
    if not symbol or "ST" in name or "退" in name or code.startswith(("8","4","9")):
        return None
    try:
        bars = fetch_kline(symbol, datalen=120)
        if not bars or len(bars) < 30:
            return None
        closes = [b["close"] for b in bars]
        highs = [b["high"] for b in bars]
        lows = [b["low"] for b in bars]
        vols = [b["volume"] for b in bars]
        k, d, j = calc_kdj(highs, lows, closes)
        if not k or math.isnan(k[-1]) or math.isnan(j[-1]):
            return None
        import numpy as np
        ma5 = np.convolve(closes, np.ones(5)/5, mode='valid').tolist()
        ma10 = np.convolve(closes, np.ones(10)/10, mode='valid').tolist()
        ma20 = np.convolve(closes, np.ones(20)/20, mode='valid').tolist()
        chgs = [0.0] + [(closes[i]-closes[i-1])/closes[i-1]*100 for i in range(1, len(closes))]
        bs = analyze_buy_sell(bars)
        vp = calc_vp_system(bars, closes, highs, lows, vols, chgs, ma5, ma10, ma20,
                            my_style=(bs or {}).get("my_style"))
        kdj_sys = calc_kdj_system(bars, closes, highs, lows, k, d, j,
                                  ma5, ma10, ma20, k, d, j, vp)
        diverge = (kdj_sys or {}).get("diverge", {})
        if diverge.get("bottom"):
            pct = float(r.get("changepercent") or 0)
            price = float(r.get("trade") or closes[-1])
            return {"code": code, "name": name, "price": round(price, 2),
                    "pct": round(pct, 2), "J": round(j[-1], 1),
                    "K": round(k[-1], 1), "D": round(d[-1], 1)}
    except Exception:
        return None
    return None

def main():
    print("正在获取全市场股票列表...")
    spot = fetch_spot_all()
    print(f"共 {len(spot)} 只股票, 开始多线程扫描(20线程)...")
    # 重置熔断器
    app._SOURCE_BREAKER['sina_down_until'] = 0
    app._SOURCE_BREAKER['fails'] = []

    results = []
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(scan_one, r): r for r in spot}
        for fut in as_completed(futs):
            done += 1
            res = fut.result()
            if res:
                results.append(res)
            if done % 500 == 0:
                print(f"  已扫描 {done}/{len(spot)}, 耗时 {time.time()-t0:.0f}s, 命中 {len(results)}")

    print(f"\n扫描完成! 共 {len(spot)} 只, 耗时 {time.time()-t0:.0f}s")
    print(f"KDJ 底背离命中 {len(results)} 只:\n")
    results.sort(key=lambda x: x["pct"])
    print(f"{'代码':<8}{'名称':<10}{'现价':>8}{'涨跌幅':>8}{'J值':>8}{'K值':>8}{'D值':>8}")
    print("-" * 60)
    for r in results:
        print(f"{r['code']:<8}{r['name']:<10}{r['price']:>8.2f}{r['pct']:>7.2f}%{r['J']:>8.1f}{r['K']:>8.1f}{r['D']:>8.1f}")

if __name__ == "__main__":
    main()
