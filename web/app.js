'use strict';

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function fmtNum(n) {
    if (n == null) return '0';
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(2) + 'K';
    return String(n);
}

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

// ── 免费模型判定 ──
const FREE_WHITELIST = new Set(['big-pickle']);

function isFreeModel(id) {
    if (!id) return false;
    return FREE_WHITELIST.has(id) || /-free$/.test(id);
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
    // 运行时设置
    mode: 'anonymous',
    hasApiKey: false,
    apiKeyMasked: null,
    // 代理详情 / 用量报表
    proxies: [],
    usage: null,
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
    } else if (name === 'playground') {
        loadModels();
    } else if (name === 'proxies') {
        fetchProxies();
    } else if (name === 'usage') {
        fetchUsage();
    } else if (name === 'apikey') {
        fetchSettings();
    }
}

// ── 运行时设置 ──
async function fetchSettings() {
    try {
        const r = await fetch('/api/settings');
        const d = await r.json();
        state.mode = d.mode;
        state.hasApiKey = d.has_api_key;
        state.apiKeyMasked = d.api_key_masked;
        renderSettings();
    } catch (_) {}
}

function renderSettings() {
    // 滑动开关状态
    const toggle = $('#modeToggle');
    if (toggle) toggle.checked = state.mode === 'authenticated';

    // 左右标签高亮
    const labelAnon = $('#modeLabelAnonymous');
    const labelAuth = $('#modeLabelAuthenticated');
    if (labelAnon && labelAuth) {
        labelAnon.classList.toggle('active', state.mode === 'anonymous');
        labelAuth.classList.toggle('active', state.mode === 'authenticated');
    }

    // API Key 页信息
    const statusEl = $('#apiKeyStatus');
    if (statusEl) {
        statusEl.textContent = state.hasApiKey
            ? (state.apiKeyMasked || '已配置')
            : '未配置';
    }
    const modeEl = $('#apiKeyMode');
    if (modeEl) {
        modeEl.textContent = state.mode === 'authenticated' ? '实名模式（直连 + Key）' : '匿名模式（代理池 + 伪装）';
    }
}

async function onModeToggle(e) {
    const wantAuth = e.target.checked;
    if (wantAuth && !state.hasApiKey) {
        toast('实名模式需要先配置 API Key', 'err');
        e.target.checked = false;
        return;
    }
    const mode = wantAuth ? 'authenticated' : 'anonymous';
    try {
        const r = await fetch('/api/settings/mode', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode }),
        });
        const d = await r.json();
        if (d.ok) {
            state.mode = d.mode;
            toast(`已切换到${wantAuth ? '实名' : '匿名'}模式`, 'ok');
            renderSettings();
            await fetchStatus();
        } else {
            toast('切换失败: ' + (d.error || '未知错误'), 'err');
            e.target.checked = state.mode === 'authenticated';
        }
    } catch (err) {
        toast('网络错误: ' + err.message, 'err');
        e.target.checked = state.mode === 'authenticated';
    }
}

async function saveApiKey() {
    const input = $('#apiKeyInput');
    const key = input.value.trim();
    if (!key) {
        toast('请输入 API Key', 'err');
        return;
    }
    try {
        const r = await fetch('/api/settings/apikey', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ api_key: key }),
        });
        const d = await r.json();
        if (d.ok) {
            toast('API Key 已保存', 'ok');
            input.value = '';
            await fetchSettings();
        } else {
            toast('保存失败: ' + (d.error || '未知错误'), 'err');
        }
    } catch (err) {
        toast('网络错误: ' + err.message, 'err');
    }
}

async function clearApiKey() {
    if (!confirm('确定清除 API Key？实名模式下将无法使用。')) return;
    try {
        const r = await fetch('/api/settings/apikey', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ api_key: null }),
        });
        const d = await r.json();
        if (d.ok) {
            toast('API Key 已清除', 'ok');
            await fetchSettings();
        } else {
            toast('清除失败', 'err');
        }
    } catch (err) {
        toast('网络错误: ' + err.message, 'err');
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
    $('#p-size').textContent = fmtExact(s.pool_size ?? 0);
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

    $('#m-total').textContent = fmtExact(m.total_requests || 0);
    $('#m-total').title = `精确值: ${fmtExact(m.total_requests || 0)} 次`;
    $('#m-success').textContent = (m.success_rate || 0).toFixed(1) + '%';
    $('#m-p95').innerHTML = (m.p95_latency || 0) + '<small>ms</small>';
    $('#m-active').textContent = fmtExact(m.active_requests || 0);
    $('#m-active-streams').textContent = '流式 ' + fmtExact(m.active_streams || 0);

    const total = m.total_tokens || 0;
    const cached = m.cached_tokens || 0;
    const reasoning = m.reasoning_tokens || 0;
    const coverage = m.usage_coverage || 0;

    $('#t-total').textContent = fmtExact(total);
    $('#t-total').title = `精确值: ${fmtExact(total)} tokens` +
        (total >= 1000 ? ` (${fmtNum(total)})` : '');

    const cachedPct = total > 0 ? Math.min((cached / total) * 100, 100) : 0;
    $('#t-cached').textContent = fmtExact(cached);
    $('#t-cached').title = `精确值: ${fmtExact(cached)} tokens · 占比 ${cachedPct.toFixed(1)}%`;
    $('#t-cached-bar').style.width = cachedPct.toFixed(1) + '%';

    const reasoningPct = total > 0 ? Math.min((reasoning / total) * 100, 100) : 0;
    $('#t-reasoning').textContent = fmtExact(reasoning);
    $('#t-reasoning').title = `精确值: ${fmtExact(reasoning)} tokens · 占比 ${reasoningPct.toFixed(1)}%`;
    $('#t-reasoning-bar').style.width = reasoningPct.toFixed(1) + '%';

    $('#t-coverage').textContent = coverage.toFixed(1) + '%';
    $('#t-coverage-bar').style.width = Math.min(coverage, 100).toFixed(1) + '%';

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

// ── 代理详情（信誉 + IP 情报） ──
async function fetchProxies() {
    try {
        const r = await fetch('/api/proxies');
        if (!r.ok) return;
        const d = await r.json();
        state.proxies = d.proxies || [];
        renderProxies();
    } catch (_) {}
}

function renderProxies() {
    const body = $('#proxyTableBody');
    if (!body) return;
    const data = state.proxies || [];
    $('#proxiesMeta').textContent = `${data.length} 个节点`;

    if (!data.length) {
        body.innerHTML = '<tr><td colspan="9" class="empty">暂无数据</td></tr>';
        return;
    }
    const sorted = [...data].sort((a, b) =>
        (b.success_count - a.success_count) ||
        ((a.risk ?? 999) - (b.risk ?? 999)));
    body.innerHTML = sorted.map((p) => {
        const risk = p.risk == null ? '-' : String(p.risk);
        const riskCls = p.risk == null ? '' :
            p.risk >= 75 ? 'err-text' : p.risk >= 40 ? 'warn-text' : 'ok-text';
        const type = p.ip_type || (p.hosting ? 'IDC' : '-');
        const isp = [p.isp, p.asn].filter(Boolean).map(escapeHtml).join('<br>') || '-';
        const status = p.available
            ? '<span class="ok-text">可用</span>'
            : `冷却 ${p.cooldown_remaining_secs ?? 0}s`;
        const lastUsed = p.last_used ? new Date(p.last_used * 1000).toLocaleString() : '-';
        return `<tr>
            <td class="mono">${escapeHtml(p.url.replace(/^https?:\/\//, ''))}</td>
            <td>${escapeHtml(p.country_code || '-')}${p.country ? `<br><span class="dim-text">${escapeHtml(p.country)}</span>` : ''}</td>
            <td class="dim-text">${isp}</td>
            <td>${escapeHtml(type)}</td>
            <td class="${riskCls}">${risk}</td>
            <td>${p.success_count} / ${p.fail_count} / ${p.ban_count}</td>
            <td>${p.latency > 0 ? p.latency.toFixed(2) + 's' : '-'}</td>
            <td>${status}</td>
            <td class="dim-text">${escapeHtml(lastUsed)}</td>
        </tr>`;
    }).join('');
}

// ── 用量报表（SQLite 明细聚合） ──
async function fetchUsage() {
    const sel = $('#usageDays');
    const days = sel ? Number(sel.value) || 14 : 14;
    try {
        const r = await fetch(`/api/usage?days=${days}`);
        if (!r.ok) return;
        state.usage = await r.json();
        renderUsageModels(state.usage.models || []);
        drawUsageChart();
    } catch (_) {}
}

function renderUsageModels(models) {
    const body = $('#usageModelBody');
    if (!body) return;
    if (!models.length) {
        body.innerHTML = '<tr><td colspan="8" class="empty">暂无数据</td></tr>';
        return;
    }
    body.innerHTML = models.map((m) => `<tr>
        <td>${escapeHtml(m.model)}</td>
        <td>${fmtExact(m.requests)}</td>
        <td>${m.requests > 0 ? (100 * m.success / m.requests).toFixed(1) + '%' : '-'}</td>
        <td>${fmtExact(m.prompt_tokens)}</td>
        <td>${fmtExact(m.completion_tokens)}</td>
        <td>${fmtExact(m.total_tokens)}</td>
        <td>${m.avg_latency_ms ? fmtNum(m.avg_latency_ms) + 'ms' : '-'}</td>
        <td>${m.avg_ttft_ms ? fmtNum(m.avg_ttft_ms) + 'ms' : '-'}</td>
    </tr>`).join('');
}

function drawUsageChart() {
    const canvas = $('#chartUsage');
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
    const data = (state.usage && state.usage.daily) || [];
    if (!data.length) {
        ctx.fillStyle = '#7d8794';
        ctx.font = '13px -apple-system, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText('暂无数据', W / 2, H / 2);
        return;
    }
    const padL = 52, padR = 12, padT = 8, padB = 22;
    const cw = W - padL - padR;
    const ch = H - padT - padB;
    const maxVal = Math.max(...data.map((d) => d[5]), 1);
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
        ctx.fillText(fmtNum(Math.round(maxVal * (1 - i / 4))), padL - 6, y);
    }
    const slotW = cw / data.length;
    const barW = Math.max(2, Math.min(22, slotW - 5));
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    const labelStep = Math.max(1, Math.ceil(data.length / 10));
    data.forEach((d, i) => {
        const v = d[5];
        const h = ch * (v / maxVal);
        const x = padL + slotW * i + (slotW - barW) / 2;
        const y = padT + ch - h;
        const grad = ctx.createLinearGradient(0, y, 0, y + h);
        grad.addColorStop(0, '#7affc4');
        grad.addColorStop(1, '#2e9e73');
        ctx.fillStyle = grad;
        ctx.fillRect(x, y, barW, h);
        if (i % labelStep === 0) {
            const date = new Date(d[0] * 86400 * 1000);
            ctx.fillStyle = '#7d8794';
            ctx.fillText(
                `${date.getMonth() + 1}/${date.getDate()}`,
                padL + slotW * i + slotW / 2,
                padT + ch + 5
            );
        }
    });
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
    const padL = 44, padR = 12, padT = 8, padB = 20;
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
    const m = line.match(/^\[([\d\- :\.]+)\]\s+\[\s*(\w+)\]\s+([\s\S]*)$/);
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
    box.innerHTML = state.upstreamModels.map((m) => {
        const free = isFreeModel(m);
        const tip = free ? '点击添加为规则' : '付费模型，不在免费层范围内';
        return `
        <span class="model-chip ${free ? '' : 'paid'}"
              data-model="${escapeHtml(m)}"
              data-free="${free}"
              title="${tip}">
            ${escapeHtml(m)}
        </span>`;
    }).join('');
    box.querySelectorAll('.model-chip').forEach((el) => {
        el.addEventListener('click', () => {
            if (el.dataset.free !== 'true') {
                toast('付费模型不在免费层范围内', 'err');
                return;
            }
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
    let allModels = [];

    try {
        const r = await fetch('/api/upstream/models');
        const d = await r.json();
        if (Array.isArray(d.models) && d.models.length > 0) {
            allModels = d.models;
        }
    } catch (_) {}

    if (allModels.length === 0) {
        try {
            const r = await fetch('/v1/models');
            const d = await r.json();
            allModels = (d.data || []).map((m) => m.id).filter(Boolean);
        } catch (_) {}
    }

    if (allModels.length === 0) {
        // 应急兜底：单一来源是 src/config.rs 的 DEFAULT_MODELS，后端正常时永远从 API 获取
        allModels = ['deepseek-v4-flash-free', 'big-pickle'];
    }

    const seen = new Set();
    const freeModels = [];
    for (const id of allModels) {
        if (!isFreeModel(id)) continue;
        if (seen.has(id)) continue;
        seen.add(id);
        freeModels.push(id);
    }

    if (freeModels.length === 0) {
        freeModels.push('big-pickle');
    }

    state.models = freeModels.map((id) => ({ id }));

    const sel = $('#pgModel');
    if (!sel) return;
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

    // 匿名/实名滑动开关
    const modeToggle = $('#modeToggle');
    if (modeToggle) modeToggle.addEventListener('change', onModeToggle);

    // API Key 管理
    const btnSaveKey = $('#btnSaveKey');
    if (btnSaveKey) btnSaveKey.addEventListener('click', saveApiKey);
    const btnClearKey = $('#btnClearKey');
    if (btnClearKey) btnClearKey.addEventListener('click', clearApiKey);
    const apiKeyInput = $('#apiKeyInput');
    if (apiKeyInput) {
        apiKeyInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                saveApiKey();
            }
        });
    }

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
    const usageDays = $('#usageDays');
    if (usageDays) usageDays.addEventListener('change', fetchUsage);
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
        else if (state.view === 'usage') drawUsageChart();
    });

    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
            fetchStatus();
            fetchMetrics();
            fetchLogs();
            fetchSettings();
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
    // 报表页数据量大，低频轮询且仅在对应视图激活时拉取
    _timers.push(setInterval(() => {
        if (state.view === 'proxies') fetchProxies();
        else if (state.view === 'usage') fetchUsage();
    }, 3000));
}
function stopPolling() {
    _timers.forEach(clearInterval);
    _timers = [];
}

async function init() {
    bindEvents();
    await Promise.all([
        fetchStatus(),
        fetchMetrics(),
        fetchLogs(),
        fetchSettings(),
        loadModels(),
    ]);
    startPolling();
}

init();