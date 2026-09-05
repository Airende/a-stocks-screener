@echo off
REM ============================================================
REM A股智能选股系统 - 本地部署启动脚本 (Windows)
REM 用法: 双击运行 或 在命令行执行 start.bat
REM ============================================================
setlocal enabledelayedexpansion
chcp 65001 >nul

REM 切换到脚本所在目录
cd /d "%~dp0"
set "ROOT=%cd%"
echo [INFO] 项目目录: %ROOT%

REM ---------- 1. 检查 Python ----------
set "PYTHON_BIN="
for %%C in (python py) do (
    where %%C >nul 2>&1
    if !errorlevel! equ 0 (
        for /f "tokens=*" %%V in ('%%C -c "import sys;print('%%d.%%d'%%(sys.version_info[0],sys.version_info[1]))" 2^>nul') do set "PYVER=%%V"
        %%C -c "import sys;exit(0 if sys.version_info>=(3,8) else 1)" >nul 2>&1
        if !errorlevel! equ 0 (
            set "PYTHON_BIN=%%C"
            echo [OK] 使用 Python: %%C (!PYVER!)
            goto :found_py
        )
    )
)
:found_py
if "%PYTHON_BIN%"=="" (
    echo [ERROR] 未找到 Python 3.8+, 请先安装: https://www.python.org/downloads/
    echo        安装时请勾选 "Add Python to PATH"
    pause
    exit /b 1
)

REM ---------- 2. 创建/复用虚拟环境 ----------
set "VENV=%ROOT%\.venv"
if not exist "%VENV%\Scripts\activate.bat" (
    echo [INFO] 创建虚拟环境 .venv ...
    %PYTHON_BIN% -m venv "%VENV%"
    if !errorlevel! neq 0 (
        echo [ERROR] 创建虚拟环境失败
        pause
        exit /b 1
    )
)
call "%VENV%\Scripts\activate.bat"

REM ---------- 3. 升级 pip ----------
echo [INFO] 升级 pip ...
python -m pip install --upgrade pip -q

REM ---------- 4. 安装依赖 ----------
if exist requirements.txt (
    echo [INFO] 安装依赖 (requirements.txt) ...
    pip install -r requirements.txt -q
) else (
    echo [WARN] 未找到 requirements.txt, 安装最小依赖 ...
    pip install fastapi "uvicorn[standard]" requests pypinyin -q
)

REM ---------- 5. 启动服务 ----------
if "%PORT%"=="" set "PORT=8000"
echo [INFO] 启动服务, 端口: %PORT%
echo [INFO] 访问地址: http://127.0.0.1:%PORT%
echo [INFO] 按 Ctrl+C 停止服务

REM 后台打开浏览器
if "%NO_BROWSER%"=="" (
    start "" /b cmd /c "timeout /t 2 /nobreak >nul && start http://127.0.0.1:%PORT%"
)

REM 启动 uvicorn
python -m uvicorn app:app --host 0.0.0.0 --port %PORT%
pause
