# -*- coding: utf-8 -*-
"""阶段1: 探测同花顺 UI 控件能否被 macOS 辅助功能接口读取。
决定走 2A(精准操控) 还是 2B(坐标盲点)。

用法(在 Mac 上):
  1. 打开同花顺, 切换到自选股/自定义板块页, 置于前台
  2. 运行:  python3 probe_ths_ui.py          # 用进程名"同花顺"
     或:    python3 probe_ths_ui.py 同花顺8   # 换你的进程名
  3. 观察输出:
     - 若列出 .txt 里的 button/text field/menu item → 走 2A, 控件可读
     - 若空白/拒绝访问/仅顶层同花顺图标 → 走 2B, 需坐标标定
"""
import subprocess, sys

# 常见进程名: 同花顺 / 同花顺8 / 同花顺9 ... 可传参覆盖
PROC = sys.argv[1] if len(sys.argv) > 1 else "同花顺"

def osa(script):
    r = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    return r.stdout, r.stderr

def main():
    print(f"目标进程: {PROC}\n" + "=" * 40)

    # 0. 先确认进程存在
    out, _ = osa(f'tell application "System Events" to get name of every process whose name contains "{PROC}"')
    print("匹配进程:", out.strip() or "(空) -> 请检查进程名/py参数")

    # 1. dump 窗口内的辅助功能元素树 (深度=3, 避免爆炸)
    print("\n--- 窗口1 控件树(深度3) ---")
    tree, err = osa(
        f'''
        tell application "System Events"
          tell process "{PROC}"
            if (count of windows) = 0 then
              return "NO_WINDOW"
            end if
            set _top to entire contents of window 1
          end tell
        end tell
        ''')
    if err:
        print("ERROR:", err.strip()); print("-> 辅助功能权限可能未授权, 检查: 系统设置>隐私>辅助功能")
        return
    shown = 0
    for line in (tree or "").splitlines():
        print(line[:200])
        shown += 1
        if shown >= 100:  # 只打印前100行避免刷屏
            print("...(截断)", flush=True); break

    # 2. 搜关键控件
    print("\n--- 关键控件检索 ---")
    for kw in ["button", "text field", "menu item", "添加", "自选", "导入", "板块"]:
        if kw in (tree or ""):
            print(f"  [命中] {kw}")

    print("\n结论参考:")
    print("  tomato 上面能列出 button/text field/菜单项  -> 走 2A(ths_ui_add_stocks.py 自动选2A)")
    print("  空/无输出/仅进程级      -> 走 2B(需先标定 ui_coords.json)")

if __name__ == "__main__":
    main()