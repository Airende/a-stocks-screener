# -*- coding: utf-8 -*-
"""导出均线形态筛选 + 上试盘 的全部分组股票清单 (供导入同花顺测试批量粘贴)
输出:
  - /workspace/ths_groups.csv   分组,代码,名称  (脚本自行读写 CSV 用)
  - /workspace/ths_groups.txt   每行"分组:: 代码 名称"多行文本, 供同花顺批量粘贴测试
运行: python3 export_ma_groups.py
"""
import requests, datetime

BASE = "http://127.0.0.1:8000"

def main():
    ma = requests.get(f"{BASE}/api/ma-screen").json()
    ssp = requests.get(f"{BASE}/api/ssp-screen").json()

    rows = []          # (group, code, name)
    text_lines = []    # 批量粘贴文本
    today = datetime.date.today().isoformat()

    # ---- 均线形态分组 ----
    for pat, lst in (ma.get("patterns") or {}).items():
        for s in lst:
            rows.append((pat, s["code"], s["name"]))
            text_lines.append(f"{pat}:: {s['code']} {s['name']}")

    # ---- 上试盘 3 池 ----
    pool_cn = {"new_signals": "上试盘-新信号", "watch_pool": "上试盘-观察池", "confirmed_pool": "上试盘-已确认"}
    for pool, cn in pool_cn.items():
        for s in (ssp.get(pool) or []):
            rows.append((cn, s["code"], s["name"]))
            text_lines.append(f"{cn}:: {s['code']} {s['name']}")

    # CSV
    with open("/workspace/ths_groups.csv", "w", encoding="utf-8-sig") as f:
        f.write("分组,代码,名称\n")
        for g, c, n in rows:
            f.write(f"{g},{c},{n}\n")

    # 批量粘贴文本
    with open("/workspace/ths_groups.txt", "w", encoding="utf-8") as f:
        f.write(f"# ARD 全部分组选股 ({today}) 共{len(rows)}只\n\n")
        f.write("\n".join(text_lines) + "\n")

    # 分组统计
    from collections import OrderedDict
    cnt = OrderedDict()
    for g, _, _ in rows:
        cnt[g] = cnt.get(g, 0) + 1
    print(f"总标的: {len(rows)} 只, {len(cnt)} 个分组\n更新: {ma.get('updated')}")
    for g, c in cnt.items():
        print(f"  {g}: {c}")

    print("\n已生成:")
    print("  /workspace/ths_groups.csv")
    print("  /workspace/ths_groups.txt")

if __name__ == "__main__":
    main()