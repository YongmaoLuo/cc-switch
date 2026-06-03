#!/usr/bin/env bash
# restart-cc-switch.sh — 安全停止并重启 CC Switch 桌面 app
#
# 端口分工（重要！）：
#   - 4000: 远程管理服务（控制面）— /api/proxy/stop、/api/usage、/api/switch 等
#   - 15721: 代理服务（数据面，Claude 流量转发）
#
# 为什么必须先关代理？
#   Claude Code 的 ~/.claude/settings.json 里 ANTHROPIC_BASE_URL 指向 127.0.0.1:15721。
#   如果 CC Switch 退出时没把代理停掉，Live 配置没回写到 provider 自己的 URL，
#   Claude 客户端调模型时 15721 端口已死 → Claude 整个挂掉。
#   /api/proxy/stop 会调用 stop_with_restore() 把 Live 配置改回 provider 自己的 URL。
#
# 步骤：
#   1. 尝试通过 4000 端口的 /api/proxy/stop 关闭代理（新二进制才有 API）
#      - 新二进制：API 成功 → 后续一切正常
#      - 老二进制：API 失败 → 仍然优雅退出 CC Switch（osascript quit 会触发 Tauri 关闭流程，
#        也会写回 Live 配置；但不保证 100% 触发 stop_with_restore）
#   2. 等待代理端口 15721 真正释放（数据面断开）
#   3. 关闭 CC Switch app（兜底）
#   4. 备份整个 .app bundle，用新构建的 .app 替换
#   5. 用 `open -n -a` 启动 app（避免当前 shell 环境变量污染 Tauri 启动）
#   6. 等待 4000 端口的远程管理服务就绪（确认新二进制已起来）
#   7. 通过同一个 /api/proxy/stop + body {"start": true} 重新拉起路由代理
#      （统一端点：stop 模式无 body → 停止；start 模式带 start:true → 启动并接管 Live）

set -euo pipefail

REMOTE_PORT=4000
PROXY_PORT=15721
APP_BUNDLE="/Applications/CC Switch.app"
APP_BINARY="${APP_BUNDLE}/Contents/MacOS/cc-switch"
SRC_TAURI="/Users/yongmaoluo/Documents/GitHub/cc-switch/src-tauri"
BUNDLE_APP="${SRC_TAURI}/target/release/bundle/macos/CC Switch.app"

# 1. 定位新 .app bundle
if [[ $# -ge 1 ]]; then
    NEW_APP="$1"
else
    NEW_APP="${BUNDLE_APP}"
fi

if [[ ! -d "${NEW_APP}" ]]; then
    echo "ERROR: 新 .app bundle 不存在：${NEW_APP}" >&2
    echo "       请先 cd ${SRC_TAURI%/*} && pnpm tauri build" >&2
    exit 1
fi

# 验证新 bundle 里有 binary
NEW_APP_BINARY="${NEW_APP}/Contents/MacOS/cc-switch"
if [[ ! -f "${NEW_APP_BINARY}" ]]; then
    echo "ERROR: 新 .app 缺少 binary：${NEW_APP_BINARY}" >&2
    exit 1
fi

echo "==> 新 .app bundle：${NEW_APP}"
echo "==> 目标路径：${APP_BUNDLE}"

# 2. 尝试通过 remote API 关闭代理（仅新二进制支持）
stop_proxy_via_api() {
    echo "==> 尝试通过 http://127.0.0.1:${REMOTE_PORT}/api/proxy/stop 关闭代理..."
    local response
    if response=$(curl -sS -X POST --max-time 5 "http://127.0.0.1:${REMOTE_PORT}/api/proxy/stop" 2>/dev/null); then
        echo "    ✓ 远程 API 关闭代理成功：${response}"
        return 0
    else
        echo "    ! 远程 API 不可用（老二进制或远程服务未启用）"
        return 1
    fi
}

# 3. 等待代理端口 15721 真正释放
wait_proxy_down() {
    echo "==> 等待代理端口 ${PROXY_PORT} 释放（数据面断开）..."
    for i in {1..15}; do
        if ! lsof -nP -iTCP:${PROXY_PORT} -sTCP:LISTEN >/dev/null 2>&1; then
            echo "    ✓ 端口 ${PROXY_PORT} 已释放"
            return 0
        fi
        sleep 1
    done
    echo "    ! 端口 ${PROXY_PORT} 在 15 秒内未释放（代理可能仍在运行）" >&2
    return 1
}

# 4. 关闭 CC Switch app
quit_cc_switch() {
    echo "==> 关闭 CC Switch app..."
    if pgrep -f "CC Switch.app/Contents/MacOS/cc-switch" >/dev/null 2>&1; then
        # 优先用 AppleScript 优雅退出（托盘菜单有"退出"项，会触发 Tauri 关闭流程）
        osascript <<'OSA' 2>/dev/null || true
tell application id "com.ccswitch.desktop" to quit
OSA
        for i in {1..10}; do
            if ! pgrep -f "CC Switch.app/Contents/MacOS/cc-switch" >/dev/null 2>&1; then
                echo "    ✓ CC Switch 已退出（优雅）"
                return 0
            fi
            sleep 1
        done
        # 优雅退出超时，改为 SIGTERM
        echo "    ! 优雅退出超时，发送 SIGTERM..."
        pkill -f "CC Switch.app/Contents/MacOS/cc-switch"
        for i in {1..5}; do
            if ! pgrep -f "CC Switch.app/Contents/MacOS/cc-switch" >/dev/null 2>&1; then
                echo "    ✓ CC Switch 已退出（SIGTERM）"
                return 0
            fi
            sleep 1
        done
        # 最后手段：SIGKILL
        echo "    ! SIGTERM 仍超时，发送 SIGKILL..." >&2
        pkill -9 -f "CC Switch.app/Contents/MacOS/cc-switch" 2>/dev/null || true
        sleep 1
    else
        echo "    (CC Switch 当前未运行)"
    fi
}

# 5. 备份并替换整个 .app bundle
replace_app_bundle() {
    local ts
    ts=$(date +%Y%m%d-%H%M%S)
    local backup_dir="/tmp/cc-switch-app-backups"
    mkdir -p "${backup_dir}"
    local backup="${backup_dir}/CC Switch.app.bak.${ts}"

    echo "==> 备份旧 .app 到：${backup}"
    rm -rf "${backup}"
    cp -R "${APP_BUNDLE}" "${backup}"

    echo "==> 用新 .app 替换..."
    rm -rf "${APP_BUNDLE}"
    cp -R "${NEW_APP}" "${APP_BUNDLE}"
    # 保留原 app 的可执行权限（cp -R 通常会保留）
    chmod +x "${APP_BINARY}"
    echo "    ✓ 新 .app 已安装：${APP_BUNDLE}"
}

# 6. 启动 CC Switch（用 `nohup env -i` 启动二进制，避免继承 shell 环境变量）
launch_cc_switch() {
    echo "==> 启动 CC Switch app（env -i 干净环境 + 必备 vars）..."
    # CC Switch 启动时会调 check_env_conflicts()，用 std::env::vars() 读取自己的进程环境。
    # 如果继承了 shell 的 ANTHROPIC_*/CLAUDECODE/CLAUDE_CODE_* 等 vars，
    # 会被它识别为"影响配置"的环境变量并弹出警告。
    #
    # 解决方案：直接 exec 二进制，但用 `env -i` 清空环境，仅注入 Tauri/webview 必需的 vars：
    #   - HOME: 配置文件、数据库位置
    #   - PATH: 子进程（如 git、node）需要
    #   - TMPDIR: macOS 临时目录约定
    #   - USER, SHELL: 让 shell 调用正常
    #   - LANG, LC_ALL: webview 文本渲染
    # 关键：不能传 ANTHROPIC_*、CLAUDECODE、CLAUDE_CODE_*、HTTPS_PROXY 等。
    #
    # 备注：`open -n -a` 也会继承 shell env；AppleScript 调 Launch Services
    # 一样会通过 osascript 进程继承 shell env（验证：ps eww 看到 CC Switch 进程里
    # 仍含 8 个 ANTHROPIC/CLAUDE vars）。所以唯一干净方式就是 `env -i` exec binary。
    # Info.plist 路径靠 binary 自身的 argv[0] 解析（macOS Mach-O load commands），
    # 不依赖环境变量。

    local home_dir
    home_dir=$(eval echo "~")

    nohup env -i \
        HOME="${home_dir}" \
        PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin" \
        TMPDIR="/tmp" \
        USER="$(whoami)" \
        SHELL="/bin/zsh" \
        LANG="${LANG:-en_US.UTF-8}" \
        LC_ALL="${LC_ALL:-en_US.UTF-8}" \
        "${APP_BINARY}" \
        >/tmp/cc-switch-launch.log 2>&1 &
    disown
    echo "    ✓ 已通过 env -i 提交启动请求（log: /tmp/cc-switch-launch.log）"
}

# 7. 等待远程服务（控制面）起来
wait_remote_ready() {
    echo "==> 等待远程服务 http://127.0.0.1:${REMOTE_PORT} 就绪（控制面）..."
    for i in {1..30}; do
        if curl -sS --max-time 2 "http://127.0.0.1:${REMOTE_PORT}/api/health" >/dev/null 2>&1; then
            echo "    ✓ 远程服务已就绪"
            return 0
        fi
        sleep 1
    done
    echo "    ! 远程服务在 30 秒内未就绪（可能未启用）" >&2
    return 1
}

# 8. 通过 RESTful API 重新拉起路由代理（接管 Live 配置）
#    步骤 1 已经把代理 stop 了（restore 了 Live 配置，并把所有 app 的 enabled 置 false），
#    重启之后代理不会自动起来，Claude Code 的 ANTHROPIC_BASE_URL
#    会保留为 provider 自己的 URL → Claude 直连上游，绕过 CC Switch。
#
#    用统一的 POST /api/proxy/stop + body {"start": true}：
#      - 单一端点既停止又开启（避免维护两套 endpoint）
#      - 内部调用 start_with_takeover() 一次性把代理拉起来 + 接管所有 app 的 Live 配置
#      - 幂等：start() 内部 is_running() 检查，set_takeover_for_app 在已接管时早退
start_proxy_via_api() {
    echo "==> 通过 POST /api/proxy/stop {\"start\": true} 重新拉起路由代理（接管 Live 配置）..."
    local response http_code
    response=$(curl -sS -X POST --max-time 10 \
        -H "Content-Type: application/json" \
        -d '{"start": true}' \
        -w "\n%{http_code}" \
        "http://127.0.0.1:${REMOTE_PORT}/api/proxy/stop" 2>&1)
    http_code=$(echo "${response}" | tail -n 1)
    response=$(echo "${response}" | sed '$d')

    # 区分三种情况：
    #   1) HTTP 非 2xx          → 网络/服务错误
    #   2) HTTP 200 + success=true  → 启动 + 接管成功
    #   3) HTTP 200 + success=false → 业务失败（如 Live 配置缺失）
    if [[ "${http_code}" =~ ^2 ]]; then
        if echo "${response}" | jq -e '.success == true' >/dev/null 2>&1; then
            echo "    ✓ 代理启动成功：${response}"
        else
            echo "    ! 代理启动业务失败：${response}" >&2
            return 1
        fi
    else
        echo "    ! 代理启动 HTTP ${http_code}：${response}" >&2
        return 1
    fi

    # 等代理端口 15721 真正起来（接管 + bind socket 需要几秒）
    for i in {1..15}; do
        if lsof -nP -iTCP:${PROXY_PORT} -sTCP:LISTEN >/dev/null 2>&1; then
            echo "    ✓ 端口 ${PROXY_PORT} 已监听"
            return 0
        fi
        sleep 1
    done
    echo "    ! 端口 ${PROXY_PORT} 在 15 秒内未监听" >&2
    return 1
}

main() {
    echo "================================================"
    echo " CC Switch 安全重启脚本"
    echo "================================================"
    echo ""
    echo "⚠️  关闭代理前请确保 /api/proxy/stop 成功，否则"
    echo "   Claude Code 的 ANTHROPIC_BASE_URL 会指向死端口，"
    echo "   导致 Claude 客户端调不通模型。"
    echo ""

    # 1. 先尝试通过 4000 端口的 API 关代理
    #    - 成功：代理状态已被 stop_with_restore() 正确处理
    #    - 失败：老二进制没这个 API，但我们仍继续（走 osascript quit 兜底）
    api_success=false
    if stop_proxy_via_api; then
        api_success=true
    fi

    # 2. 等代理端口 15721 释放
    wait_proxy_down || true

    # 3. 关 app
    quit_cc_switch

    # 4. 换 .app bundle
    replace_app_bundle

    # 5. 启动（用 open，纯净环境变量）
    launch_cc_switch

    # 6. 等远程服务（仅新二进制会有）
    wait_remote_ready || true

    # 7. 重新拉起路由代理（接管 Live 配置）
    #    仅当 step 1 的 stop 成功时才需要 — 成功说明老二进制支持新 API，
    #    新二进制也支持 /api/proxy/takeover 配套恢复。
    if [[ "${api_success}" == "true" ]]; then
        start_proxy_via_api || true
    else
        echo ""
        echo "ℹ️  跳过代理恢复（老二进制不支持 /api/proxy/stop，"
        echo "   新二进制接管后请手动在 GUI 启用代理或调 /api/proxy/takeover）。"
    fi

    echo ""
    echo "================================================"
    echo " 完成。验证："
    echo "   curl http://127.0.0.1:${REMOTE_PORT}/api/health"
    echo "   curl http://127.0.0.1:${REMOTE_PORT}/api/usage"
    if [[ "${api_success}" == "false" ]]; then
        echo ""
        echo "⚠️  首次重启时 /api/proxy/stop 不可用，Claude Code 的"
        echo "   ANTHROPIC_BASE_URL 可能仍指向 127.0.0.1:15721。"
        echo "   建议检查 ~/.claude/settings.json，必要时手动改为"
        echo "   provider 自己的 URL（Anthropic / Zhipu 等）。"
    fi
    echo "================================================"
}

main "$@"
