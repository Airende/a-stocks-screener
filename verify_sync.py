# -*- coding: utf-8 -*-
"""阶段3: 质量闭环比对。
对比「期望(ths_groups.csv / run_log.csv)」与「同花顺里实际同步回来」的股票, 输出差异报告。

⚠️ 同花顺本地板块文件格式不透明, 我无法直接读它的二进制。
因此这里设计了两种实际验证方式, 请你按可用情况二选一:

方式1(推荐, 免读文件): 你在同花顺里把自定义板块导出/另存一份文本(若它能导出txt/csv),
   把该文件路径作为参数传入, 与期望逐条比对。
方式2: 只核对 run_log.csv 是否全部 ok (脚本侧自检)。
"""
import csv, os, sys, argparse
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--actual", help="同花顺导出的文本/csv 路径")
    ap.add_argument("--expect", default=os.path.join(BASE, "ths_groups.csv"), help="期望清单")
    ap.add_argument("--log", default=os.path.join(BASE, "run_log.csv"), help="执行日志")
    return ap.parse_args()

def parse_expect(path):
    """期望: 分组,代码[,名称] -> set(代码)"""
    s = set()
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.reader(f):
            if len(row) >= 2 and row[1].strip().isdigit():
                s.add(row[1].zfill(6))
    return s

def parse_actual(path):
    """实际: 尽量宽松抓取 6 位数字作为股票代码"""
    s = set()
    import re
    with open(path, encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            for tok in re.findall(r'\b\d{6}\b', line):
                s.add(tok)
    return s

def check_log(path):
    """校验 run_log 是否全部 ok / 有无 fail"""
    stat = defaultdict(int); fails = []
    if not os.path.exists(path):
        print(f"[log] {path} 不存在, 先运行主脚本"); return
    with open(path, encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) < 3: continue
            stat[row[2]] += 1
            if row[2] == "fail":
                fails.append((row[0], row[1]))
    print(f"[run_log] 状态分布: {dict(stat)}")
    if fails:
        print(f"[run_log] 失败 {len(fails)} 条: {fails[:20]}")

def main():
    a = parse_args()
    print("=" * 50)
    check_log(a.log)

    if not a.actual:
        print("\n未提供 --actual (同花顺导出文本), 无法做差异比对。")
        print("要么: 同花顺导出板块文本后, 再跑:")
        print("  python3 verify_sync.py --actual /path/to/ths_export.txt")
        return

    exp = parse_expect(a.expect)
    act = parse_actual(a.actual)
    missing = exp - act          # 期望了但实际没有
    extra = act - exp            # 实际多了(疑似误加/脏数据)
    print(f"\n期望 {len(exp)} 只 | 实际 {len(act)} 只")
    print(f"命中: {len(exp & act)} 只 | 缺失(未同步上): {len(missing)} 只 | 多余: {len(extra)} 只")

    if missing:
        print("\n--- 以下股票未同步成功, 请补跑 ---")
        print(", ".join(sorted(missing)))
    if extra:
        print("\n--- 实际中疑似多余(需人工确认) ---")
        print(", ".join(sorted(extra)))
    print("\n" + ("✅ 全量同步成功" if not missing and not extra else "❌ 存在差异"))

if __name__ == "__main__":
    main()