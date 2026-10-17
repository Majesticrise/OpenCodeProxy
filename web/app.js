'use strict';

// ── 工具 ──
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])
    );
}

function fmtNum(n) {
    if (n == null) return '0';
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(2) + 'K';
    return String(n);
}

// ── 全局状态 ──
const state = {
    view: 'dashboard',
    status: null,
    metrics: null,
    allLogs: [],      // 后端返回的完整日志数组
    renderFrom: 0,    // 从该索引开始显示（清空日志时用）
    levelFilter: 'all',
    search: '',
    paused: false,
    autoScroll: true,
    models: [],
    pgSending: false,
    pgAccum: '',
};

// ── 视图切换 ──
function switchView(name) {
    state.view = name;
    $$('.nav-item').forEach((el) => el.classList.toggle('active', el.dataset.view === name));
    $$('.view').forEach((el) => el.classList.toggle('active', el.id === 'view-' + name));
    if (name === 'dashboard') {
        // 视图可见后再绘制图表，否则 getBoundingClientRect 返回 0
        requestAnimationFrame(() => drawChart());
    } else if (name === 'logs') {
        const box = $('#logBox');
        if (state.autoScroll) box.scrollTop = box.scrollHeight;
    }
}

// ── 状态轮询 ──
async function fetchStatus() {
    try {
        const r = await fetch('/api/status');
        if (!r.ok) return;
        state.status = await r.json();
        renderStatus();
    } catch (_) {
        // 静默忽略：网络抖动时保持上一次的状态
    }
}

function renderStatus() {
    const s = state.status;
    if (!s) return;

    const running = !!s.running;
    const dot = $('#statusDot');
    dot.className = 'dot ' + (running ? 'ok' : 'off');
    $('#statusLabel').textContent = running ? '运行中' : '已停止';

    const btn = $('#toggleBtn');
    btn.textContent = running ? '停止服务' : '启动服务';
    btn.className = 'btn ' + (running ? 'btn-danger' : 'btn-primary');

    $('#p-size').textContent = s.pool_size ?? 0;
    $('#p-latency').textContent = s.avg_latency > 0 ? s.avg_latency.toFixed(2) + 's' : '-';
    $('#p-state').textContent = s.fetching
        ? '正在测速…'
        : running
          ? '正常'
          : '已停止';

    $('#metaPort').textContent = ':' + (location.port || '8910');
}

async function toggleService() {
    const btn = $('#toggleBtn');
    btn.disabled = true;
    try {
        await fetch('/api/toggle', { method: 'POST' });
        await fetchStatus();
    } catch (e) {
        console.error('toggle 失败', e);
    } finally {
        btn.disabled = false;
    }
}

// ── 指标 ──
async function fetchMetrics() {
    try {
        const r = await fetch('/api/metrics');
        if (!r.ok) return;
        state.metrics = await r.json();
        renderMetrics();
        if (state.view === 'dashboard') drawChart();
    } catch (_) {}
}

function renderMetrics() {
    const m = state.metrics;
    if (!m) return;

    $('#m-total').textContent = fmtNum(m.total_requests || 0);
    $('#m-success').textContent = (m.success_rate || 0).toFixed(1) + '%';
    $('#m-p95').innerHTML = (m.p95_latency || 0) + '<small>ms</small>';
    $('#m-active').textContent = m.active_requests || 0;
    $('#m-active-streams').textContent = '流式 ' + (m.active_streams || 0);

    $('#t-total').textContent = fmtNum(m.total_tokens || 0);
    $('#t-cached').textContent = fmtNum(m.cached_tokens || 0);
    $('#t-reasoning').textContent = fmtNum(m.reasoning_tokens || 0);
    $('#t-coverage').textContent = (m.usage_coverage || 0).toFixed(1) + '%';

    renderModelList(m.models || []);
    renderStatusList(m.endpoints || []);
}

function renderModelList(models) {
    const box = $('#modelList');
    if (!models.length) {
        box.innerHTML = '<div class="empty">暂无数据</div>';
        return;
    }
    const sorted = [...models].sort((a, b) => b[1][2] - a[1][2]);
    const max = Math.max(...sorted.map((x) => x[1][2]), 1);
    box.innerHTML = sorted
        .map(([name, v]) => {
            const pct = Math.max(2, (v[2] / max) * 100);
            return `<div class="bar-row">
                <div class="bar-label" title="${escapeHtml(name)}">${escapeHtml(name)}</div>
                <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
                <div class="bar-val">${fmtNum(v[2])}</div>
            </div>`;
        })
        .join('');
}

function renderStatusList(endpoints) {
    const box = $('#statusList');
    if (!endpoints.length) {
        box.innerHTML = '<div class="empty">暂无数据</div>';
        return;
    }
    const sorted = [...endpoints].sort((a, b) => a[0] - b[0]);
    const max = Math.max(...sorted.map((x) => x[1]), 1);
    box.innerHTML = sorted
        .map(([code, count]) => {
            const pct = Math.max(2, (count / max) * 100);
            const cls = code < 300 ? 'ok' : code < 400 ? 'warn' : 'err';
            return `<div class="bar-row">
                <div class="bar-label">HTTP ${code}</div>
                <div class="bar-track"><div class="bar-fill ${cls}" style="width:${pct}%"></div></div>
                <div class="bar-val">${count}</div>
            </div>`;
        })
        .join('');
}

// ── 每分钟 Token 图表 ──
function drawChart() {
    const canvas = $('#chartPerMinute');
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) return;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(rect.width * dpr);
    canvas.height = Math.round(rect.height * dpr);
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const W = rect.width;
    const H = rect.height;
    ctx.clearRect(0, 0, W, H);

    const data = (state.metrics && state.metrics.per_minute) || [];

    if (!data.length) {
        ctx.fillStyle = '#5c6470';
        ctx.font = '13px -apple-system, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText('暂无数据', W / 2, H / 2);
        return;
    }

    const padL = 44, padR = 12, padT = 10, padB = 22;
    const cw = W - padL - padR;
    const ch = H - padT - padB;
    const maxVal = Math.max(...data.map((d) => d[1][2]), 1);

    // 网格 + Y 轴标签
    ctx.strokeStyle = '#232832';
    ctx.lineWidth = 1;
    ctx.font = '10px -apple-system, sans-serif';
    ctx.fillStyle = '#5c6470';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= 4; i++) {
        const y = padT + (ch * i) / 4 + 0.5;
        ctx.beginPath();
        ctx.moveTo(padL, y);
        ctx.lineTo(padL + cw, y);
        ctx.stroke();
        ctx.fillText(fmtNum(Math.round(maxVal * (1 - i / 4))), padL - 6, y);
    }

    // 柱状
    const slotW = cw / data.length;
    const barW = Math.max(2, Math.min(18, slotW - 4));
    data.forEach((d, i) => {
        const v = d[1][2];
        const h = ch * (v / maxVal);
        const x = padL + slotW * i + (slotW - barW) / 2;
        const y = padT + ch - h;

        const grad = ctx.createLinearGradient(0, y, 0, y + h);
        grad.addColorStop(0, '#7aa2ff');
        grad.addColorStop(1, '#3b63c9');
        ctx.fillStyle = grad;
        ctx.fillRect(x, y, barW, h);
    });

    // X 轴基线
    ctx.strokeStyle = '#303643';
    ctx.beginPath();
    ctx.moveTo(padL, padT + ch + 0.5);
    ctx.lineTo(padL + cw, padT + ch + 0.5);
    ctx.stroke();
}

// ── 日志 ──
// 后端日志格式：[  1234.567] [ INFO] 消息内容
function parseLogLine(line) {
    const m = line.match(/^\[(\s*\d+\.\d+)\]\s+\[\s*(\w+)\]\s+([\s\S]*)$/);
    if (m) return { ts: m[1].trim(), level: m[2].toUpperCase(), msg: m[3] };
    return { ts: '', level: 'INFO', msg: line };
}

async function fetchLogs() {
    if (state.paused) return;
    try {
        const r = await fetch('/api/logs');
        if (!r.ok) return;
        const logs = await r.json();
        state.allLogs = logs;

        // 缓冲区滚动后，原起点可能已失效，重置为 0
        if (logs.length < state.renderFrom) state.renderFrom = 0;

        renderLogs(logs.slice(state.renderFrom));
    } catch (_) {}
}

function renderLogs(lines) {
    const box = $('#logBox');
    const parsed = lines.map(parseLogLine);

    const filtered = parsed.filter((l) => {
        if (state.levelFilter !== 'all' && l.level !== state.levelFilter) return false;
        if (state.search && !l.msg.toLowerCase().includes(state.search.toLowerCase())) {
            return false;
        }
        return true;
    });

    box.innerHTML = filtered
        .map((l) => {
            let cls = 'info';
            if (l.level === 'ERROR') cls = 'error';
            else if (l.level === 'WARN') cls = 'warn';
            else if (l.level === 'DEBUG' || l.level === 'TRACE') cls = 'dim';
            return `<div class="log-line ${cls}">
                <span class="log-ts">${escapeHtml(l.ts)}</span>
                <span class="log-level">${escapeHtml(l.level)}</span>
                <span class="log-msg">${escapeHtml(l.msg)}</span>
            </div>`;
        })
        .join('');

    $('#logCount').textContent = `${filtered.length} / ${parsed.length} 条`;

    if (state.autoScroll) box.scrollTop = box.scrollHeight;
}

function clearLogs() {
    state.renderFrom = state.allLogs.length;
    renderLogs([]);
}

// ── Playground ──
async function loadModels() {
    try {
        const r = await fetch('/v1/models');
        const d = await r.json();
        state.models = d.data || [];
    } catch (_) {
        state.models = [
            { id: 'deepseek-v4-flash-free' },
            { id: 'big-pickle' },
            { id: 'mimo-v2.5-free' },
            { id: 'nemotron-3-super-free' },
            { id: 'minimax-m3-free' },
        ];
    }
    const sel = $('#pgModel');
    sel.innerHTML = state.models
        .map((m) => `<option value="${escapeHtml(m.id)}">${escapeHtml(m.id)}</option>`)
        .join('');
}

async function sendPlayground() {
    if (state.pgSending) return;

    const model = $('#pgModel').value;
    const text = $('#pgInput').value.trim();
    if (!text) return;

    const stream = $('#pgStream').checked;
    const out = $('#pgResponse');
    const sendBtn = $('#pgSend');

    state.pgSending = true;
    state.pgAccum = '';
    out.textContent = '';
    out.classList.remove('error');
    sendBtn.disabled = true;
    sendBtn.textContent = '发送中…';

    try {
        const res = await fetch('/v1/chat/completions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                model,
                messages: [{ role: 'user', content: text }],
                stream,
            }),
        });

        if (!res.ok) {
            const errText = await res.text();
            out.textContent = `HTTP ${res.status}\n${errText}`;
            out.classList.add('error');
            return;
        }

        if (stream) {
            await readStream(res, out);
        } else {
            const j = await res.json();
            const content =
                j?.choices?.[0]?.message?.content ?? JSON.stringify(j, null, 2);
            out.textContent = content;
        }
    } catch (e) {
        out.textContent = '请求失败: ' + (e?.message || e);
        out.classList.add('error');
    } finally {
        state.pgSending = false;
        sendBtn.disabled = false;
        sendBtn.textContent = '发送';
    }
}

async function readStream(res, out) {
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';

    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });

        // 按行处理，保留最后一段不完整的行
        const lines = buf.split('\n');
        buf = lines.pop() || '';

        for (const raw of lines) {
            const line = raw.trim();
            if (!line.startsWith('data:')) continue;
            const data = line.slice(5).trim();
            if (data === '[DONE]') continue;
            try {
                const j = JSON.parse(data);
                const delta = j?.choices?.[0]?.delta?.content;
                if (delta) {
                    state.pgAccum += delta;
                    out.textContent = state.pgAccum;
                    out.scrollTop = out.scrollHeight;
                }
            } catch (_) {
                // 忽略无法解析的片段
            }
        }
    }
}

// ── 初始化 ──
function bindEvents() {
    $$('.nav-item').forEach((el) =>
        el.addEventListener('click', () => switchView(el.dataset.view))
    );

    $('#toggleBtn').addEventListener('click', toggleService);

    // 日志工具栏
    $$('#levelFilter .chip').forEach((chip) =>
        chip.addEventListener('click', () => {
            $$('#levelFilter .chip').forEach((c) => c.classList.remove('active'));
            chip.classList.add('active');
            state.levelFilter = chip.dataset.level;
            renderLogs(state.allLogs.slice(state.renderFrom));
        })
    );

    $('#logSearch').addEventListener('input', (e) => {
        state.search = e.target.value;
        renderLogs(state.allLogs.slice(state.renderFrom));
    });

    $('#logPause').addEventListener('change', (e) => {
        state.paused = e.target.checked;
        if (!state.paused) fetchLogs();
    });

    $('#logAutoScroll').addEventListener('change', (e) => {
        state.autoScroll = e.target.checked;
    });

    $('#logClear').addEventListener('click', clearLogs);

    // Playground
    $('#pgSend').addEventListener('click', sendPlayground);
    $('#pgClear').addEventListener('click', () => {
        $('#pgInput').value = '';
        $('#pgResponse').textContent = '';
        $('#pgResponse').classList.remove('error');
    });

    // 文本域 Ctrl+Enter 快捷发送
    $('#pgInput').addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            sendPlayground();
        }
    });

    window.addEventListener('resize', () => {
        if (state.view === 'dashboard') drawChart();
    });
}

async function init() {
    bindEvents();

    await Promise.all([fetchStatus(), fetchMetrics(), fetchLogs(), loadModels()]);

    setInterval(fetchStatus, 1000);
    setInterval(fetchMetrics, 1000);
    setInterval(fetchLogs, 1000);
}

init();