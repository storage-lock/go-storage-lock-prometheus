package go_storage_lock_prometheus

import (
	"net/http"
)

// DashboardHandler 返回一个零依赖的 HTML 看板：用浏览器 fetch 轮询同源
// /metrics，把 storage-lock 的关键指标渲染成卡片。不需要 Grafana/Prometheus
// 也能单独打开 http://host:port/dashboard 查看锁服务健康度。
func (x *PrometheusListener) DashboardHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(dashboardHTML))
	})
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>storage-lock 看板</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 24px; background: #0f172a; color: #e2e8f0; }
  h1 { font-size: 20px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 12px; margin-top: 16px; }
  .card { background: #1e293b; border-radius: 8px; padding: 14px; }
  .card h3 { margin: 0 0 6px; font-size: 13px; color: #94a3b8; }
  .card .value { font-size: 26px; font-weight: 600; }
  .card .sub { font-size: 12px; color: #64748b; margin-top: 4px; }
  .ok { color: #34d399; } .warn { color: #fbbf24; } .bad { color: #f87171; }
  .table { margin-top: 16px; border-collapse: collapse; font-size: 13px; }
  .table th, .table td { border: 1px solid #334155; padding: 6px 10px; text-align: left; }
  .table th { background: #1e293b; color: #94a3b8; }
  #status { margin-top: 8px; font-size: 12px; color: #64748b; }
</style>
</head>
<body>
<h1>storage-lock 看板 <span id="status"></span></h1>
<div class="grid" id="grid"></div>
<table class="table" id="table"><thead><tr>
  <th>指标</th><th>storage</th><th>值</th><th>含义</th>
</tr></thead><tbody id="tbody"></tbody></table>

<script>
const EXPLAIN = {
  "storage_lock_acquire_total": "获取尝试（result）",
  "storage_lock_contention_total": "争抢失败（kind）",
  "storage_lock_release_total": "释放（result）",
  "storage_lock_watchdog_renew_total": "看门狗续租（result）",
  "storage_lock_waiters": "当前排队等待者总数",
  "storage_lock_contended_locks": "存在等待者的锁数量",
  "storage_lock_hold_seconds_quantile": "持有时长分位数（s）",
};

async function poll() {
  try {
    const res = await fetch('/metrics');
    const text = await res.text();
    render(text);
    document.getElementById('status').textContent = '更新于 ' + new Date().toLocaleTimeString();
  } catch (e) {
    document.getElementById('status').textContent = '连接失败: ' + e;
  }
}

function valueOf(lines, name, labelKey, labelVal) {
  const prefix = name + '{';
  for (const line of lines) {
    if (!line.startsWith(prefix)) continue;
    const open = line.indexOf('{');
    const close = line.indexOf('}');
    if (open < 0 || close < 0) continue;
    const labels = line.slice(open + 1, close);
    const m = labels.match(new RegExp('(?:^|[," ])' + labelKey + '="' + labelVal + '"'));
    if (m) return parseFloat(line.slice(close + 1));
  }
  return 0;
}

function render(text) {
  const lines = text.split('\n').filter(l => l && !l.startsWith('#'));
  const grid = document.getElementById('grid');
  const tbody = document.getElementById('tbody');
  grid.innerHTML = '';
  tbody.innerHTML = '';

  const waiters = valueOf(lines, 'storage_lock_waiters', 'x', 'x');
  const contended = valueOf(lines, 'storage_lock_contended_locks', 'x', 'x');
  const busy = valueOf(lines, 'storage_lock_acquire_total', 'result', 'busy');
  const renewErr = valueOf(lines, 'storage_lock_watchdog_renew_total', 'result', 'error');
  const ops = valueOf(lines, 'storage_lock_operations_total', 'result', 'success');

  const cards = [
    { t: '排队等待者', v: waiters, cls: waiters > 0 ? 'warn' : 'ok', s: 'storage_lock_waiters' },
    { t: '争抢中的锁', v: contended, cls: contended > 0 ? 'warn' : 'ok', s: 'storage_lock_contended_locks' },
    { t: '获取冲突(busy)', v: busy, cls: busy > 0 ? 'warn' : 'ok', s: 'storage_lock_acquire_total' },
    { t: '续租失败', v: renewErr, cls: renewErr > 0 ? 'bad' : 'ok', s: 'storage_lock_watchdog_renew_total' },
    { t: '成功操作', v: ops, cls: 'ok', s: 'storage_lock_operations_total' },
  ];
  for (const c of cards) {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = '<h3>' + c.t + '</h3><div class="value ' + c.cls + '">' + c.v + '</div><div class="sub">' + EXPLAIN[c.s] + '</div>';
    grid.appendChild(div);
  }

  // 每个 {metric{storage="..."}} 一行，展开全部低基数维度。
  const seen = new Set();
  for (const line of lines) {
    const open = line.indexOf('{');
    if (open < 0) continue;
    const close = line.indexOf('}');
    if (close < 0) continue;
    const name = line.slice(0, open);
    if (!EXPLAIN[name]) continue;
    const labels = line.slice(open + 1, close);
    const st = (labels.match(/storage="([^"]*)"/) || [])[1] || '-';
    const rest = labels.replace(/storage="[^"]*"/, '').replace(/^,/, '').replace(/,$/, '');
    const key = name + '|' + st + '|' + rest;
    if (seen.has(key)) continue;
    seen.add(key);
    const tr = document.createElement('tr');
    tr.innerHTML = '<td>' + name + '</td><td>' + st + '</td><td>' +
      parseFloat(line.slice(close + 1)) + '</td><td>' + (rest ? rest : EXPLAIN[name]) + '</td>';
    tbody.appendChild(tr);
  }
}

poll();
setInterval(poll, 2000);
</script>
</body>
</html>
`
