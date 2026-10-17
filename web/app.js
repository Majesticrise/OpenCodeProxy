'use strict';

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

/** 缩写格式：190.09K / 1.23M（用于图表 Y 轴等空间受限场景） */
function fmtNum(n) {
    if (n == null) return '0';
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(2) + 'K';
    return String(n);
}

/** 精确格式：带千分位，190,090（用于 Token、请求数等核心指标） */
function fmtExact(n) {
    if (n == null) return '0';
    const num = Number(n);
    if (!Number.isFinite(num)) return '0';
    return num.toLocaleString('en-US');
}

function toast(msg, kind = '') {
    let el = document.getElementById('_toast');
    if (!el) {
        el = document.createElement('div');
        el.id = '_toast';
        el.className = 'toast';
        document.body.appendChild(el);
    }
    el.textContent = msg;
    el.className = `toast ${kind} show`;
    clearTimeout(el._t);
    el._t = setTimeout(() => el.classList.remove('show'), 2600);
}

const state = {
    view: 'dashboard',
    status: null,
    metrics: null,
    allLogs: [],
    renderFrom: 0,
    levelFilter: 'all',
    search: '',
    paused: false,
    autoScroll: true,
    models: [],
    pgSending: false,
    pgAccum: '',
    upstreamModels: [],
    routeRows: [],
    enableFallback: true,
};

// ── 视图切换 ──
function switchView(name) {
    state.view = name;
    $$('.nav-item').forEach((el) => el.classList.toggle('active', el.dataset.view === name));
    $$('.view').forEach((el) => el.classList.toggle('active', el.id === 'view-' + name));
    if (name === 'dashboard') requestAnimationFrame(() => drawChart());
    else if (name === 'logs') {
        const box = $('#logBox');
        if (state.autoScroll) box.scrollTop = box.scrollHeight;
    } else if (name === 'routing') {
        loadRouting();
        loadUpstreamModels();
    }
}

// ── 状态 ──
let _statusFails = 0;
async function fetchStatus() {
    try {
        const r = await fetch('/api/status');
        if (!r.ok) throw new Error(r.status);
        _statusFails = 0;
        state.status = await r.json();
        renderStatus();
    } catch (_) {
        _statusFails++;
        if (_statusFails >= 3) {
            $('#statusDot').className = 'dot off';
            $('#statusLabel').textContent = '后端无响应';
        }
    }
}

function renderStatus() {
    const s = state.status;
    if (!s) return;
    const running = !!s.running;
    $('#statusDot').className = 'dot ' + (running ? 'ok' : 'off');
    $('#statusLabel').textContent = running ? '运行中' : '已停止';
    const btn = $('#toggleBtn');
    btn.textContent = running ? '停止服务' : '启动服务';
    btn.className = 'btn ' + (running ? 'btn-danger' : 'btn-primary');
    $('#p-size').textContent = s.pool_size ?? 0;
    $('#p-latency').textContent = s.avg_latency > 0 ? s.avg_latency.toFixed(2) + 's' : '-';
    $('#p-state').textContent = s.fetching ? '正在测速…' : (running ? '正常' : '已停止');
    $('#metaPort').textContent = ':' + (location.port || '8910');
}

async function toggleService() {
    const btn = $('#toggleBtn');
    btn.disabled = true;
    try {
        await fetch('/api/toggle', { method: 'POST' });
        await fetchStatus();
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

    // 总请求数、Token 相关：精确数字
    $('#m-total').textContent = fmtExact(m.total_requests || 0);
    $('#m-total').title = `精确值: ${fmtExact(m.total_requests || 0)} 次`;
    $('#m-success').textContent = (m.success_rate || 0).toFixed(1) + '%';
    $('#m-p95').innerHTML = (m.p95_latency || 0) + '<small>ms</small>';
    $('#m-active').textContent = fmtExact(m.active_requests || 0);
    $('#m-active-streams').textContent = '流式 ' + fmtExact(m.active_streams || 0);

    const totalTokens = m.total_tokens || 0;
    const cachedTokens = m.cached_tokens || 0;
    const reasoningTokens = m.reasoning_tokens || 0;

    $('#t-total').textContent = fmtExact(totalTokens);
    $('#t-total').title = `精确值: ${fmtExact(totalTokens)} tokens` +
        (totalTokens >= 1000 ? ` (${fmtNum(totalTokens)})` : '');

    $('#t-cached').textContent = fmtExact(cachedTokens);
    $('#t-cached').title = `精确值: ${fmtExact(cachedTokens)} tokens` +
        (totalTokens > 0 ? ` · 占比 ${((cachedTokens / totalTokens) * 100).toFixed(1)}%` : '');

    $('#t-reasoning').textContent = fmtExact(reasoningTokens);
    $('#t-reasoning').title = `精确值: ${fmtExact(reasoningTokens)} tokens` +
        (totalTokens > 0 ? ` · 占比 ${((reasoningTokens / totalTokens) * 100).toFixed(1)}%` : '');

    $('#t-coverage').textContent = (m.usage_coverage || 0).toFixed(1) + '%';

    renderModelList(m.models || []);
    renderStatusList(m.endpoints || []);
}

function renderModelList(models) {
    const box = $('#modelList');
    if (!models.length) { box.innerHTML = '<div class="empty">暂无数据</div>'; return; }
    const sorted = [...models].sort((a, b) => b[1][2] - a[1][2]);
    const max = Math.max(...sorted.map((x) => x[1][2]), 1);
    box.innerHTML = sorted.map(([name, v]) => {
        const pct = Math.max(2, (v[2] / max) * 100);
        const exact = fmtExact(v[2]);
        const tip = `${name}\nprompt: ${fmtExact(v[0])}\ncompletion: ${fmtExact(v[1])}\ntotal: ${exact}`;
        return `<div class="bar-row" title="${escapeHtml(tip)}">
            <div class="bar-label" title="${escapeHtml(name)}">${escapeHtml(name)}</div>
            <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
            <div class="bar-val">${exact}</div>
        </div>`;
    }).join('');
}

function renderStatusList(endpoints) {
    const box = $('#statusList');
    if (!endpoints.length) { box.innerHTML = '<div class="empty">暂无数据</div>'; return; }
    const sorted = [...endpoints].sort((a, b) => a[0] - b[0]);
    const max = Math.max(...sorted.map((x) => x[1]), 1);
    box.innerHTML = sorted.map(([code, count]) => {
        const pct = Math.max(2, (count / max) * 100);
        const cls = code < 300 ? 'ok' : code < 400 ? 'warn' : 'err';
        return `<div class="bar-row">
            <div class="bar-label">HTTP ${code}</div>
            <div class="bar-track"><div class="bar-fill ${cls}" style="width:${pct}%"></div></div>
            <div class="bar-val">${fmtExact(count)}</div>
        </div>`;
    }).join('');
}

// ── 图表 ──
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
    const W = rect.width, H = rect.height;
    ctx.clearRect(0, 0, W, H);
    const data = (state.metrics && state.metrics.per_minute) || [];
    if (!data.length) {
        ctx.fillStyle = '#7d8794';
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
    ctx.strokeStyle = '#2f3643';
    ctx.lineWidth = 1;
    ctx.font = '10px -apple-system, sans-serif';
    ctx.fillStyle = '#7d8794';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= 4; i++) {
        const y = padT + (ch * i) / 4 + 0.5;
        ctx.beginPath();
        ctx.moveTo(padL, y);
        ctx.lineTo(padL + cw, y);
        ctx.stroke();
        // Y 轴保持缩写（空间有限）
        ctx.fillText(fmtNum(Math.round(maxVal * (1 - i / 4))), padL - 6, y);
    }
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
}

// ── 日志 ──
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
        if (logs.length < state.renderFrom) state.renderFrom = 0;
        renderLogs(logs.slice(state.renderFrom));
    } catch (_) {}
}

function renderLogs(lines) {
    const box = $('#logBox');
    const parsed = lines.map(parseLogLine);
    const filtered = parsed.filter((l) => {
        if (state.levelFilter !== 'all' && l.level !== state.levelFilter) return false;
        if (state.search && !l.msg.toLowerCase().includes(state.search.toLowerCase())) return false;
        return true;
    });
    box.innerHTML = filtered.map((l) => {
        let cls = 'info';
        if (l.level === 'ERROR') cls = 'error';
        else if (l.level === 'WARN') cls = 'warn';
        else if (l.level === 'DEBUG' || l.level === 'TRACE') cls = 'dim';
        return `<div class="log-line ${cls}">
            <span class="log-ts">${escapeHtml(l.ts)}</span>
            <span class="log-level">${escapeHtml(l.level)}</span>
            <span class="log-msg">${escapeHtml(l.msg)}</span>
        </div>`;
    }).join('');
    $('#logCount').textContent = `${filtered.length} / ${parsed.length} 条`;
    if (state.autoScroll) box.scrollTop = box.scrollHeight;
}

function clearLogs() {
    state.renderFrom = state.allLogs.length;
    renderLogs([]);
}

// ── 模型路由 ──
async function loadUpstreamModels(forceRefresh = false) {
    const box = $('#upstreamModels');
    const meta = $('#upstreamMeta');
    if (forceRefresh) {
        meta.textContent = '探测中…';
        box.innerHTML = '<div class="empty">正在请求上游…</div>';
        try {
            const r = await fetch('/api/upstream/refresh', { method: 'POST' });
            const d = await r.json();
            if (!d.ok) {
                meta.textContent = '探测失败：' + (d.error || '未知错误');
                box.innerHTML = '<div class="empty">探测失败</div>';
                return;
            }
            state.upstreamModels = d.models || [];
            meta.textContent = `共 ${state.upstreamModels.length} 个 · 来源 ${d.source || '?'}`;
        } catch (e) {
            meta.textContent = '探测失败';
            box.innerHTML = '<div class="empty">网络错误</div>';
            return;
        }
    } else {
        try {
            const r = await fetch('/api/upstream/models');
            const d = await r.json();
            state.upstreamModels = d.models || [];
            if (state.upstreamModels.length) {
                const age = d.age_secs != null ? `${d.age_secs}s 前` : '已缓存';
                meta.textContent = `共 ${state.upstreamModels.length} 个 · ${age} · 来源 ${d.source || '?'}`;
            } else if (d.last_error) {
                meta.textContent = '上次探测失败：' + d.last_error;
            } else {
                meta.textContent = '未探测';
            }
        } catch (_) {}
    }

    if (!state.upstreamModels.length) {
        box.innerHTML = '<div class="empty">点击"刷新上游列表"获取</div>';
        return;
    }
    box.innerHTML = state.upstreamModels.map((m) => `
        <span class="model-chip" data-model="${escapeHtml(m)}" title="点击添加为规则">
            ${escapeHtml(m)}
        </span>
    `).join('');
    box.querySelectorAll('.model-chip').forEach((el) => {
        el.addEventListener('click', () => {
            const m = el.dataset.model;
            if (state.routeRows.length === 0) {
                state.routeRows.push({ key: m, candidates: m });
            } else {
                const target = state.routeRows.find((r) => r.key !== '*') || state.routeRows[0];
                const existing = target.candidates.split(',').map((s) => s.trim()).filter(Boolean);
                if (!existing.includes(m)) existing.push(m);
                target.candidates = existing.join(',');
            }
            renderRouteTable();
        });
    });
}

async function loadRouting() {
    try {
        const r = await fetch('/api/routing');
        const d = await r.json();
        state.enableFallback = d.enable_fallback_chain !== false;
        $('#enableFallback').checked = state.enableFallback;
        state.routeRows = Object.entries(d.mapping || {}).map(([k, v]) => ({
            key: k,
            candidates: Array.isArray(v) ? v.join(',') : String(v),
        }));
    } catch (_) {
        state.routeRows = [];
    }
    if (state.routeRows.length === 0) {
        state.routeRows.push({ key: '', candidates: '' });
    }
    renderRouteTable();
}

function renderRouteTable() {
    const tbody = $('#routeTableBody');
    if (state.routeRows.length === 0) {
        state.routeRows.push({ key: '', candidates: '' });
    }
    tbody.innerHTML = state.routeRows.map((row, i) => `
        <tr data-idx="${i}">
            <td>
                <input type="text" class="r-key" value="${escapeHtml(row.key)}"
                       placeholder="例如 deepseek-v4-flash-free 或 *" />
                ${row.key === '*' ? '<span class="key-badge">兜底</span>' : ''}
            </td>
            <td>
                <input type="text" class="r-cand" value="${escapeHtml(row.candidates)}"
                       placeholder="a,b,c（按优先级排序）" />
            </td>
            <td>
                <button class="row-del" data-idx="${i}" title="删除">✕</button>
            </td>
        </tr>
    `).join('');

    tbody.querySelectorAll('tr').forEach((tr) => {
        const idx = +tr.dataset.idx;
        tr.querySelector('.r-key').addEventListener('input', (e) => {
            state.routeRows[idx].key = e.target.value;
        });
        tr.querySelector('.r-cand').addEventListener('input', (e) => {
            state.routeRows[idx].candidates = e.target.value;
        });
        tr.querySelector('.row-del').addEventListener('click', () => {
            state.routeRows.splice(idx, 1);
            renderRouteTable();
        });
    });
}

async function saveRouting() {
    const mapping = {};
    for (const row of state.routeRows) {
        const key = row.key.trim();
        const cands = row.candidates.split(',').map((s) => s.trim()).filter(Boolean);
        if (!key || cands.length === 0) continue;
        mapping[key] = cands;
    }

    if (Object.keys(mapping).length === 0) {
        if (!confirm('路由表为空，保存后会退回到"原样透传"模式。继续？')) return;
    }

    const payload = {
        mapping,
        enable_fallback_chain: $('#enableFallback').checked,
    };

    try {
        const r = await fetch('/api/routing', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        if (!r.ok) {
            toast('保存失败：' + r.status, 'err');
            return;
        }
        const d = await r.json();
        if (d.ok) {
            toast('路由已更新并生效', 'ok');
            await loadModels();
        } else {
            toast('保存失败', 'err');
        }
    } catch (e) {
        toast('网络错误：' + e.message, 'err');
    }
}

// ── Playground ──
async function loadModels() {
    try {
        const r = await fetch('/v1/models');
        const d = await r.json();
        state.models = d.data || [];
    } catch (_) {
        state.models = [{ id: 'deepseek-v4-flash-free' }];
    }
    const sel = $('#pgModel');
    const prev = sel.value;
    sel.innerHTML = state.models
        .map((m) => `<option value="${escapeHtml(m.id)}">${escapeHtml(m.id)}</option>`)
        .join('');
    if (prev && state.models.some((m) => m.id === prev)) sel.value = prev;
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
            body: JSON.stringify({ model, messages: [{ role: 'user', content: text }], stream }),
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
            const content = j?.choices?.[0]?.message?.content ?? JSON.stringify(j, null, 2);
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
            } catch (_) {}
        }
    }
}

// ── 事件绑定与启动 ──
function bindEvents() {
    $$('.nav-item').forEach((el) =>
        el.addEventListener('click', () => switchView(el.dataset.view)));
    $('#toggleBtn').addEventListener('click', toggleService);

    $$('#levelFilter .chip').forEach((chip) =>
        chip.addEventListener('click', () => {
            $$('#levelFilter .chip').forEach((c) => c.classList.remove('active'));
            chip.classList.add('active');
            state.levelFilter = chip.dataset.level;
            renderLogs(state.allLogs.slice(state.renderFrom));
        }));
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

    $('#btnProbe').addEventListener('click', () => loadUpstreamModels(true));
    $('#btnAddRule').addEventListener('click', () => {
        state.routeRows.push({ key: '', candidates: '' });
        renderRouteTable();
    });
    $('#btnSaveRouting').addEventListener('click', saveRouting);

    $('#pgSend').addEventListener('click', sendPlayground);
    $('#pgClear').addEventListener('click', () => {
        $('#pgInput').value = '';
        $('#pgResponse').textContent = '';
        $('#pgResponse').classList.remove('error');
    });
    $('#pgInput').addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            sendPlayground();
        }
    });

    window.addEventListener('resize', () => {
        if (state.view === 'dashboard') drawChart();
    });

    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
            fetchStatus();
            fetchMetrics();
            fetchLogs();
            startPolling();
        } else {
            stopPolling();
        }
    });
}

let _timers = [];
function startPolling() {
    stopPolling();
    _timers.push(setInterval(fetchStatus, 1000));
    _timers.push(setInterval(fetchMetrics, 1000));
    _timers.push(setInterval(fetchLogs, 1000));
}
function stopPolling() {
    _timers.forEach(clearInterval);
    _timers = [];
}

async function init() {
    bindEvents();
    await Promise.all([fetchStatus(), fetchMetrics(), fetchLogs(), loadModels()]);
    startPolling();
}

init();