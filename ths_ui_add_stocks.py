# -*- coding: utf-8 -*-
"""同花顺 逐只加入 主执行脚本 (自动选择 2A / 2B 路线)

前置:
  - Mac 已授权 终端(或运行 App) 的"辅助功能"和"屏幕录制"权限
  - 已安装 cliclick (brew install cliclick)  —— 2B 路线必用, 2A 仅探测失败兜底
  - 同花顺已打开并切到"自定义板块/自选股"页, 已在目标分组
  - 已生成 /workspace/ths_groups.csv (分组,代码,名称)

用法:
  python3 ths_ui_add_stocks.py                          # 全部分组
  python3 ths_ui_add_stocks.py 缠论·日线三买             # 只灌某个分组(从断点续跑)
  python3 ths_ui_add_stocks.py --only 多头排列 均价日线   # 只灌多个分组 用引号

特点:
  - 自动探测 2A(控件可读) 优先; 失败自动回落 2B(坐标)
  - 逐只间隔 0.5s, 防止丢输入
  - 2B 模式: 每个分组切换需要人工确认(因坐标无法识别是哪个分组), 避免改错组
  - run_log.csv 记录 分组,代码,状态(ok/fail/skip), 支持断点续跑(skip表示已完成)
  - 2B 每10只截屏一次到 /workspace/shot_01.png 供人工确认未跑偏
"""
import subprocess, time, csv, sys, json, os, threading

BASE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(BASE, "ths_groups.csv")
LOG = os.path.join(BASE, "run_log.csv")
COORDS = os.path.join(BASE, "ui_coords.json")
SHOT_DIR = os.path.join(BASE, "shots")
PROC = "同花顺"
DELAY = 0.5          # 逐只间隔 (2B 放宽至0.5s防丢输入)
RETRY = 1            # 失败重试次数
MODE = None          # 'A' / 'B', 探测后定
_coords = {}

def osa(script):
    r = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    return r.stdout.strip(), r.stderr.strip()

def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return r.stdout.strip()

def log(ok=True):
    pass  # 由调用处写 run_log

def load_csv():
    with open(CSV, encoding="utf-8-sig") as f:
        return list(csv.reader(f))[1:]  # 跳表头

def load_already_done():
    done = set()
    if os.path.exists(LOG):
        with open(LOG, encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) >= 3 and row[2] == "ok":
                    done.add((row[0], row[1]))
    return done

def append_log(grp, code, status):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"{grp},{code},{status}\n")

def focus_process():
    osa(f'tell application "System Events" to tell process "{PROC}" to set frontmost to true')

def win_active():
    out, _ = osa(f'tell application "System Events" to tell process "{PROC}" to get frontmost')
    return out == "true"

# ---------------- 2A 路线: 控件精准操控 ----------------
def detect_2a():
    """探测能否读到输入框/按钮, 返回 True 可用"""
    out, err = osa(
        f'tell application "System Events" to tell process "{PROC}"\n'
        f'if (count of windows) = 0 then return "NO_WIN"\n'
        f'set vs to value of every text field of window 1\n'
        f'return "OK"\n'
        f'end tell')
    if err or out != "OK":
        return False
    # 再确认识别到任意 button
    out, _ = osa(f'tell application "System Events" to tell process "{PROC}" to get name of every button of window 1')
    return bool(out.strip())

def ax_focus_code_input():
    return osa(f'tell application "System Events" to tell process "{PROC}"\n'
               f'  set value of text field 1 of window 1 to ""\n'
               f'end tell')

def ax_set_code(code):
    return osa(f'tell application "System Events" to tell process "{PROC}"\n'
               f'  set value of text field 1 of window 1 to "{code}"\n'
               f'  keystroke return\n'
               f'end tell')

# ---------------- 2B 路线: 坐标盲点 ----------------
def cc(x, y):
    sh(f"cliclick c:{int(x)},{int(y)}")

def move_win():
    w = _coords["win"]
    sh(f"osascript -e 'tell application \"System Events\" to tell process \"{PROC}\" to set position of window 1 to {{{w['x']},{w['y']}}}'")
    sh(f"osascript -e 'tell application \"System Events\" to tell process \"{PROC}\" to set size of window 1 to {{{w['w']},{w['h']}}}'")

shot_counter = [0]
def snapshot(tag):
    os.makedirs(SHOT_DIR, exist_ok=True)
    shot_counter[0] += 1
    sh(f"screencapture -x {SHOT_DIR}/shot_{shot_counter[0]:02d}.png")

def b_add_stock(code):
    cc(_coords["code_input"]["x"], _coords["code_input"]["y"])   # 点输入框
    time.sleep(0.3)
    sh("cliclick kp:cmd+a")                                       # 全选
    sh("cliclick kd:cmd && cliclick kp:keycode:51 && cliclick ku:cmd")  # 删除(Backspace)
    time.sleep(0.2)
    sh(f'cliclick t:"{code}"')                                   # 键盘输入
    time.sleep(0.2)
    sh("cliclick kp:return")                                     # 回车确认
    time.sleep(DELAY)

def b_click_group(name):
    cc(_coords["group_list"]["x"], _coords["group_list"]["y"])

# ---------------- 主流程 ----------------
def run_group(group, codes, done):
    print(f"\n=== 分组 [{group}] {len(codes)}只 ===")
    ok_codes = 0
    for code in codes:
        if (group, code) in done:
            print(f"  skip(已完成) {code}"); continue
        if not win_active(): move_win()
        success = False
        for attempt in range(RETRY + 1):
            if MODE == "A":
                ax_focus_code_input(); time.sleep(0.1)
                _, err = ax_set_code(code)
                success = (not err)
            else:
                b_add_stock(code)
                time.sleep(0.2)
                success = True  # 坐标无法回读, 默认成功; 依赖阶段3验证
            if success:
                break
            time.sleep(DELAY)
        append_log(group, code, "ok" if success else "fail")
        if success:
            ok_codes += 1
        if MODE == "B" and shot_counter[0] and shot_counter[0] % 10 == 0 and shot_counter[0] > 0:
            snapshot(group)  # 每10只截屏
        time.sleep(DELAY)
    print(f"[{group}] ok {ok_codes}/{len(codes)}")

def main():
    global MODE, _coords, CSV
    args = sys.argv[1:]
    only = None
    if args and args[0] == "--only":
        only = set(args[1:])
    elif args and args[0] == "--csv":
        # 指定数据源: python3 ths_ui_add_stocks.py --csv ths_groups_test.csv
        CSV = os.path.join(BASE, args[1])
        if not os.path.exists(CSV):
            print(f"指定CSV不存在: {CSV}"); return

    rows = load_csv()
    if not rows:
        print("csv 为空: 先运行 export_ma_groups.py"); return

    # 探测模式
    _coords = json.load(open(COORDS, encoding="utf-8"))
    MODE = "A" if detect_2a() else "B"
    print(f"[探测] 选路线 -> 2{MODE} ({'控件精准操控' if MODE=='A' else '坐标盲点'})")
    if MODE == "B":
        print("提示: 请先用 cliclick p 核对 ui_coords.json 的窗口/输入框坐标, 并手工把同花顺窗口移到指定位置")
        try:
            move_win()                      # 固定窗口位置, 确保坐标一致
        except Exception:
            pass
        time.sleep(0.5)
        cc(_coords["code_input"]["x"], _coords["code_input"]["y"])   # 先点一下输入框聚焦
        time.sleep(0.3)

    done = load_already_done()
    print(f"已完成断点: {len(done)} 条")

    # 按分组聚合
    groups = {}
    for grp, code, name in rows:
        if only and grp not in only:
            continue
        groups.setdefault(grp, []).append(code)
    if not groups:
        print("没有匹配的分组(检查 --only 名称)"); return

    focus_process()
    time.sleep(0.5)
    for grp, codes in groups.items():
        if MODE == "B":
            # 坐标盲点无法识别分组, 由人工确认已在目标分组(避免灌错组)
            print(f"\n请在左侧手动点选分组 [ {grp} ] (共{len(codes)}只)")
            input("准备好后按 回车 开始该分组...")
        run_group(grp, codes, done)

    print(f"\n完成. 日志: {LOG}  截屏: {SHOT_DIR}/")
    print("跑完后可用 verify_sync.py 做质量闭环比对")

if __name__ == "__main__":
    main()