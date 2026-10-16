let isRunning = true;
let lastLogCount = 0;

async function fetchStatus() {
    try {
        const res = await fetch('/api/status');
        const data = await res.json();
        
        isRunning = data.running;

        // 更新状态栏
        document.getElementById('statusText').innerHTML = 
            `服务状态 <span class="${isRunning ? 'green' : 'yellow'}">● ${isRunning ? '运行中' : '已停止'}</span>`;
        document.getElementById('poolText').innerText = 
            `活动代理池 ${data.pool_size} 个节点 ${data.fetching ? '(正在测速)' : ''}`;
        document.getElementById('reqText').innerText = `已处理请求 ${data.total_requests}`;
        document.getElementById('latencyText').innerText = 
            `平均延迟 ${data.avg_latency > 0 ? data.avg_latency.toFixed(2) + 's' : '-'}`;

        // 更新按钮样式
        const btn = document.getElementById('toggleBtn');
        btn.innerText = isRunning ? '停止代理服务' : '启动代理服务';
        btn.className = isRunning ? 'btn-stop' : 'btn-stop running';
        
    } catch (e) {
        console.error("获取状态失败", e);
    }
}

async function fetchLogs() {
    try {
        const res = await fetch('/api/logs');
        const logs = await res.json();
        const box = document.getElementById('logBox');
        const autoScroll = document.getElementById('autoScroll').checked;

        // 只有日志数量变化时才重绘，减少性能消耗
        if (logs.length !== lastLogCount) {
            box.innerHTML = logs.map(line => {
                // 简单的颜色高亮
                let colorClass = 'info';
                if (line.includes('[WARN]')) colorClass = 'warn';
                if (line.includes('[ERROR]')) colorClass = 'error';
                return `<div class="${colorClass}">${line}</div>`;
            }).join('');
            
            lastLogCount = logs.length;
            if (autoScroll) box.scrollTop = box.scrollHeight;
        }
    } catch (e) {
        console.error("获取日志失败", e);
    }
}

async function toggleService() {
    await fetch('/api/toggle', { method: 'POST' });
    await fetchStatus();
}

function clearLogs() {
    document.getElementById('logBox').innerHTML = '';
    lastLogCount = -1; // 强制下次重绘
}

// 定时刷新
setInterval(fetchStatus, 1000);
setInterval(fetchLogs, 1000);

// 初始加载
fetchStatus();
fetchLogs();
