function iptestApp() {
  return {
    view: 'list',
    tasks: [],
    tasksRequestID: 0,
    tasksLoading: false,
    reportRequestID: 0,
    reportLoading: false,
    pendingActions: {},
    toast: '',
    pollHandle: null,
    reportTimer: null,
    currentReportID: null,
    report: null,
    charts: {},
    probeScheduleStr: '60,60,60,120,300,600',
    form: defaultForm(),
    showSystem: false,
    sysInfo: null,

    async init() {
      await this.loadTasks();
      this.refreshIcons();
      this.pollHandle = setInterval(() => this.loadTasks(true), 2000);

      this.$watch('view', v => {
        this.refreshIcons();
        if (v !== 'report') this.stopReportAutoRefresh();
      });
      this.$watch('tasks', () => this.refreshIcons());
      this.$watch('report', () => this.refreshIcons());
    },

    refreshIcons() {
      this.$nextTick(() => {
        if (window.lucide && window.lucide.createIcons) window.lucide.createIcons();
      });
    },

    goNew() {
      this.resetForm();
      this.view = 'new';
    },
    resetForm() {
      this.form = defaultForm();
      this.probeScheduleStr = '60,60,60,120,300,600';
    },

    async loadTasks(silent = false) {
      if (silent && this.tasksLoading) return;
      const requestID = ++this.tasksRequestID;
      this.tasksLoading = true;
      try {
        const res = await fetch('api/tasks');
        const data = await res.json();
        if (requestID !== this.tasksRequestID) return;
        if (!res.ok || !Array.isArray(data)) throw new Error(data.error || '任务列表响应异常');
        this.tasks = data;
      } catch (e) {
        if (!silent && requestID === this.tasksRequestID) this.notify('加载失败: ' + e.message);
      } finally {
        if (requestID === this.tasksRequestID) this.tasksLoading = false;
      }
    },

    successRate(t) {
      const total = t.running?.total_reqs ?? t.total_requests;
      const success = t.running?.success ?? t.total_success;
      if (!total) return '-';
      return ((success / total) * 100).toFixed(1) + '%';
    },

    failureRate(t) {
      if (!t || !t.total_requests) return '-';
      return ((t.total_failed / t.total_requests) * 100).toFixed(2) + '%';
    },

    liveQps(t) {
      const now = Date.now();
      const curTotal = t.running?.total_reqs ?? t.total_requests;
      if (!t._lastSnap) {
        t._lastSnap = { time: now, total: curTotal };
        return '-';
      }
      const dt = (now - t._lastSnap.time) / 1000;
      if (dt < 1) return '-';
      const qps = (curTotal - t._lastSnap.total) / dt;
      t._lastSnap = { time: now, total: curTotal };
      return qps > 0 ? qps.toFixed(0) : '0';
    },

    formatLatencyForRow(t) {
      const ms = t.running?.avg_latency_ms ?? t.avg_latency_ms;
      if (!ms) return '-';
      return (ms / 1000).toFixed(1) + 's';
    },

    formatLatencyMs(ms) {
      if (ms == null) return '-';
      return (ms / 1000).toFixed(1) + 's';
    },

    dupRateRow(t) {
      const r = t.running;
      const total = (r?.success ?? t.total_success) || 0;
      const ips = (r?.unique_ips ?? t.unique_ips) || 0;
      if (total <= 0 || ips <= 0) return '-';
      return (((total - ips) / total) * 100).toFixed(1) + '%';
    },

    pctOf(n, d) {
      if (!d) return '-';
      return ((n / d) * 100).toFixed(1) + '%';
    },

    formatDuration(sec) {
      sec = +sec || 0;
      if (sec <= 0) return '-';
      const h = Math.floor(sec / 3600);
      const m = Math.floor((sec % 3600) / 60);
      const s = sec % 60;
      if (h) return `${h}h ${m}m ${s}s`;
      if (m) return `${m}m ${s}s`;
      return `${s}s`;
    },

    formatTime(ms) {
      if (!ms) return '-';
      return new Date(ms).toLocaleString('zh-CN', { hour12: false });
    },

    sessidModeLabel(mode) {
      return mode === 'sequential' ? '递增循环' : '随机';
    },

    nextSessidLabel(task) {
      if (task?.config?.sessid_range?.mode !== 'sequential') return '-';
      const range = task.config.sessid_range;
      let next = task.sweep_next_sessid ?? range.min;
      if (next < range.min || next > range.max) next = range.min;
      return Number(next).toLocaleString();
    },

    async createTask(autoStart = false) {
      const sched = this.probeScheduleStr
        .split(/[,\s]+/)
        .map(s => parseInt(s.trim()))
        .filter(n => n > 0);
      this.form.stickiness_probe.poll_schedule_seconds = sched.length ? sched : [60];

      const cfg = this.toConfig();
      if (!cfg.proxy.template || !cfg.proxy.template.includes('{num}')) {
        this.notify('代理模板必须包含 {num} 占位符');
        return;
      }
      const editingId = this.form._editingId;
      try {
        if (editingId) {
          const res = await fetch(`api/tasks/${editingId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: this.form.name, config: cfg }),
          });
          const data = await res.json();
          if (data.error) {
            this.notify('保存失败: ' + data.error);
            return;
          }
          this.notify(`✅ 已保存修改 #${editingId}`);
          await this.loadTasks();
          this.view = 'list';
          this.resetForm();
          return;
        }
        const res = await fetch('api/tasks', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: this.form.name, config: cfg }),
        });
        const data = await res.json();
        if (data.error) {
          this.notify('创建失败: ' + data.error);
          return;
        }
        this.notify('✅ 已创建任务 #' + data.id);
        if (autoStart) {
          const startRes = await fetch(`api/tasks/${data.id}/start`, { method: 'POST' });
          const started = await startRes.json();
          if (!startRes.ok || !started.ok || started.error) {
            this.notify(`任务 #${data.id} 已创建，启动失败: ` + (started.error || startRes.status));
          } else {
            this.notify('🚀 已开始任务 #' + data.id);
          }
        }
        await this.loadTasks();
        this.view = 'list';
        this.resetForm();
      } catch (e) {
        this.notify('保存失败: ' + e.message);
      }
    },

    toConfig() {
      const f = this.form;
      f.proxy.template = normalizeProxyTemplate(f.proxy.template);
      return {
        proxy: {
          scheme: f.proxy.scheme || 'socks5',
          template: f.proxy.template,
        },
        target_url: f.target_url,
        sessid_range: { ...f.sessid_range },
        duration_seconds: f.duration_seconds | 0,
        concurrency: f.concurrency | 0,
        request_timeout_ms: f.request_timeout_ms | 0,
        pool_sweep: {
          enabled: !!f.pool_sweep.enabled,
          stop_when_hit_rate_above: +f.pool_sweep.stop_when_hit_rate_above || 0,
          hit_rate_window_size: f.pool_sweep.hit_rate_window_size | 0,
        },
        stickiness_probe: {
          enabled: !!f.stickiness_probe.enabled,
          sessid_count: f.stickiness_probe.sessid_count | 0,
          poll_schedule_seconds: f.stickiness_probe.poll_schedule_seconds || [60],
          loop_schedule: !!f.stickiness_probe.loop_schedule,
        },
      };
    },

    async act(id, action) {
      if (this.pendingActions[id]) return;
      this.pendingActions[id] = true;
      try {
        const res = await fetch(`api/tasks/${id}/${action}`, { method: 'POST' });
        const data = await res.json();
        if (!res.ok || !data.ok || data.error) {
          this.notify(data.error || `操作失败 (${res.status})`);
        } else {
          this.notify(`✅ ${action}`);
          await this.loadTasks();
        }
      } catch (e) {
        this.notify('失败: ' + e.message);
      } finally {
        delete this.pendingActions[id];
      }
    },

    async confirmDelete(id) {
      if (!confirm(`确认删除任务 #${id}？所有数据将一并清除。`)) return;
      await fetch(`api/tasks/${id}`, { method: 'DELETE' });
      await this.loadTasks();
      this.notify('已删除任务 #' + id);
    },

    async confirmClear(id) {
      if (!confirm(`确认清空任务 #${id} 的数据？任务保留，但所有样本/指标删除。`)) return;
      await fetch(`api/tasks/${id}/clear`, { method: 'POST' });
      await this.loadTasks();
      this.notify('已清空数据 #' + id);
    },

    async cloneTask(id) {
      try {
        const task = this.tasks.find(t => t.id === id)
          || (await fetch(`api/tasks/${id}`).then(r => r.json()));
        if (!task || task.error) {
          this.notify('源任务读取失败');
          return;
        }
        const cfg = task.config || {};
        const f = defaultForm();
        f._editing = true;
        f.name = (task.name || '') + ' (副本)';
        f.target_url = cfg.target_url || f.target_url;
        f.proxy.scheme = cfg.proxy?.scheme || 'socks5';
        f.proxy.template = cfg.proxy?.template || '';
        f.sessid_range = { ...f.sessid_range, ...(cfg.sessid_range || {}) };
        f.duration_seconds = cfg.duration_seconds ?? f.duration_seconds;
        f.concurrency = cfg.concurrency ?? f.concurrency;
        f.request_timeout_ms = cfg.request_timeout_ms ?? f.request_timeout_ms;
        f.pool_sweep = { ...f.pool_sweep, ...(cfg.pool_sweep || {}) };
        f.stickiness_probe = { ...f.stickiness_probe, ...(cfg.stickiness_probe || {}) };
        this.form = f;
        const sched = cfg.stickiness_probe?.poll_schedule_seconds || [60];
        this.probeScheduleStr = sched.join(',');
        this.view = 'new';
        this.notify('已带入参数，点保存生成新任务');
      } catch (e) {
        this.notify('复制失败: ' + e.message);
      }
    },

    openSystem() {
      this.showSystem = !this.showSystem;
      if (this.showSystem) this.loadSystem();
    },

    async loadSystem() {
      try {
        const r = await fetch('api/system');
        this.sysInfo = await r.json();
        this.refreshIcons();
      } catch (e) {
        this.notify('加载系统状态失败');
      }
    },

    async editTask(id) {
      try {
        const task = this.tasks.find(t => t.id === id)
          || (await fetch(`api/tasks/${id}`).then(r => r.json()));
        if (!task || task.error) {
          this.notify('任务读取失败');
          return;
        }
        if (task.state === 'running') {
          this.notify('运行中任务不能修改，请先暂停');
          return;
        }
        const cfg = task.config || {};
        const f = defaultForm();
        f._editingId = id;
        f.name = task.name || '';
        f.target_url = cfg.target_url || f.target_url;
        f.proxy.scheme = cfg.proxy?.scheme || 'socks5';
        f.proxy.template = cfg.proxy?.template || '';
        f.sessid_range = { ...f.sessid_range, ...(cfg.sessid_range || {}) };
        f.duration_seconds = cfg.duration_seconds ?? f.duration_seconds;
        f.concurrency = cfg.concurrency ?? f.concurrency;
        f.request_timeout_ms = cfg.request_timeout_ms ?? f.request_timeout_ms;
        f.pool_sweep = { ...f.pool_sweep, ...(cfg.pool_sweep || {}) };
        f.stickiness_probe = { ...f.stickiness_probe, ...(cfg.stickiness_probe || {}) };
        this.form = f;
        const sched = cfg.stickiness_probe?.poll_schedule_seconds || [60];
        this.probeScheduleStr = sched.join(',');
        this.view = 'new';
      } catch (e) {
        this.notify('编辑失败: ' + e.message);
      }
    },

    async openReport(id) {
      this.stopReportAutoRefresh();
      this.view = 'report';
      this.currentReportID = id;
      const t = this.tasks.find(x => x.id === id);
      if (t) {
        this.report = {
          task: t,
          countries: [], errors: [], top_ips: [],
          stickiness: [], stickiness_ge3min: 0, stickiness_ge10min: 0,
          probe_sessions: [], dup_rate: 0, avg_per_ip: 0, max_dup_count: 0,
          _loading: true,
        };
      }
      await this.loadReport(id);
      if (this.view === 'report' && this.currentReportID === id) this.startReportAutoRefresh();
    },

    startReportAutoRefresh() {
      this.stopReportAutoRefresh();
      this.reportTimer = setInterval(() => {
        if (this.view !== 'report' || !this.currentReportID) {
          this.stopReportAutoRefresh();
          return;
        }
        const t = this.tasks.find(x => x.id === this.currentReportID);
        if (t && (t.state === 'running' || this.report?.task?.state === 'running')) {
          this.loadReport(this.currentReportID, true);
        }
      }, 3000);
    },

    stopReportAutoRefresh() {
      if (this.reportTimer) {
        clearInterval(this.reportTimer);
        this.reportTimer = null;
      }
    },

    async loadReport(id, silent = false) {
      if (silent && this.reportLoading) return;
      const requestID = ++this.reportRequestID;
      this.reportLoading = true;
      const isCurrent = () => requestID === this.reportRequestID
        && this.view === 'report' && this.currentReportID === id;
      try {
        const t0 = Date.now();
        if (!silent) this.notify('报表加载中...');
        const [report, metrics] = await Promise.all([
          fetch(`api/tasks/${id}/report`).then(async r => {
            const data = await r.json();
            if (!r.ok || data.error) throw new Error(data.error || `报表请求失败 (${r.status})`);
            return data;
          }),
          fetch(`api/tasks/${id}/metrics`).then(async r => {
            const data = await r.json();
            if (!r.ok || !Array.isArray(data)) throw new Error(data.error || '指标响应异常');
            return data;
          }),
        ]);
        if (!isCurrent()) return;
        if (report.task?.id !== id) throw new Error('报表任务不匹配');
        this.report = { ...report, _loading: false };
        this.$nextTick(() => {
          if (!isCurrent()) return;
          this.drawCharts(metrics);
          this.refreshIcons();
        });
        if (!silent) {
          const dt = ((Date.now() - t0) / 1000).toFixed(1);
          this.notify(`✅ 报表加载完成 (${dt}s)`);
        }
      } catch (e) {
        if (!silent && isCurrent()) this.notify('加载报表失败: ' + e.message);
      } finally {
        if (requestID === this.reportRequestID) this.reportLoading = false;
      }
    },

    drawCharts(metrics) {
      Object.values(this.charts).forEach(c => c?.destroy?.());
      this.charts = {};
      Chart.defaults.color = '#374151';
      Chart.defaults.borderColor = '#e5e7eb';

      const tsCtx = document.getElementById('chart-ts');
      if (tsCtx && metrics?.length) {
        const labels = metrics.map(p => new Date(p.ts).toLocaleTimeString('zh-CN', { hour12: false }));
        this.charts.ts = new Chart(tsCtx, {
          type: 'line',
          data: {
            labels,
            datasets: [
              { label: '独立 IP', data: metrics.map(p => p.unique_ips), borderColor: '#16a34a', backgroundColor: 'rgba(22,163,74,.1)', yAxisID: 'y', tension: 0.25, pointRadius: 0 },
              { label: '重复度 (%)', data: metrics.map(p => +(p.hit_rate * 100).toFixed(2)), borderColor: '#d97706', backgroundColor: 'rgba(217,119,6,.1)', yAxisID: 'y1', tension: 0.25, pointRadius: 0 },
              { label: 'QPS', data: metrics.map(p => +p.qps.toFixed(1)), borderColor: '#2563eb', backgroundColor: 'rgba(37,99,235,.1)', yAxisID: 'y2', tension: 0.25, pointRadius: 0 },
            ],
          },
          options: {
            responsive: true, interaction: { intersect: false, mode: 'index' },
            plugins: { legend: { labels: { color: '#374151' } } },
            scales: {
              x: { ticks: { color: '#6b7280', maxTicksLimit: 10 } },
              y: { position: 'left', ticks: { color: '#16a34a' }, title: { display: true, text: '独立IP', color: '#16a34a' } },
              y1: { position: 'right', ticks: { color: '#d97706' }, grid: { display: false }, min: 0, max: 100, title: { display: true, text: '重复度%', color: '#d97706' } },
              y2: { position: 'right', ticks: { color: '#2563eb' }, grid: { display: false }, title: { display: true, text: 'QPS', color: '#2563eb' }, offset: true },
            },
          },
        });
      }

      const cCtx = document.getElementById('chart-country');
      const countries = this.report?.countries || [];
      if (cCtx && countries.length) {
        const top = countries.slice(0, 12);
        this.charts.country = new Chart(cCtx, {
          type: 'doughnut',
          data: {
            labels: top.map(c => c.country),
            datasets: [{ data: top.map(c => c.count), backgroundColor: chartColors(top.length), borderWidth: 1, borderColor: '#fff' }],
          },
          options: { plugins: { legend: { labels: { color: '#374151', boxWidth: 12 }, position: 'right' } } },
        });
      }

      const eCtx = document.getElementById('chart-error');
      const errors = this.report?.errors || [];
      if (eCtx && errors.length) {
        this.charts.err = new Chart(eCtx, {
          type: 'doughnut',
          data: {
            labels: errors.map(e => e.kind),
            datasets: [{ data: errors.map(e => e.count), backgroundColor: ['#dc2626','#d97706','#6b7280','#2563eb','#a855f7'], borderWidth: 1, borderColor: '#fff' }],
          },
          options: { plugins: { legend: { labels: { color: '#374151', boxWidth: 12 }, position: 'right' } } },
        });
      } else if (eCtx) {
        const ctx = eCtx.getContext('2d');
        ctx.clearRect(0,0,eCtx.width,eCtx.height);
        ctx.fillStyle = '#16a34a'; ctx.font = '14px sans-serif';
        ctx.fillText('✓ 无错误记录', 20, 40);
      }

      const sCtx = document.getElementById('chart-stick');
      const sticks = this.report?.stickiness || [];
      if (sCtx && sticks.length) {
        this.charts.stick = new Chart(sCtx, {
          type: 'bar',
          data: {
            labels: sticks.map(b => b.label),
            datasets: [{ label: 'sessid 数', data: sticks.map(b => b.count), backgroundColor: '#2563eb' }],
          },
          options: {
            plugins: { legend: { labels: { color: '#374151' } } },
            scales: {
              x: { ticks: { color: '#6b7280' } },
              y: { ticks: { color: '#6b7280' }, beginAtZero: true },
            },
          },
        });
      }
    },

    notify(msg) {
      this.toast = msg;
      clearTimeout(this._toastT);
      this._toastT = setTimeout(() => (this.toast = ''), 3000);
    },
  };
}

function defaultForm() {
  return {
    _editing: false,
    _editingId: null,
    name: '',
    target_url: 'https://api.skyvora.cc/appapi/geo/cityinfo',
    proxy: {
      scheme: 'socks5',
      template: 'user-zphdrj4g-region-us-sessid-{num}-sesstime-600:U9NR5Dr4@gate3.abcsocks.cloud:9102',
    },
    sessid_range: { min: 1000000, max: 9000000, mode: 'random' },
    duration_seconds: 1800,
    concurrency: 500,
    request_timeout_ms: 15000,
    pool_sweep: {
      enabled: true,
      stop_when_hit_rate_above: 0,
      hit_rate_window_size: 1000,
    },
    stickiness_probe: {
      enabled: true,
      sessid_count: 30,
      poll_schedule_seconds: [60, 60, 60, 120, 300, 600],
      loop_schedule: true,
    },
  };
}

function normalizeProxyTemplate(template) {
  const s = template.trim();
  const at = s.lastIndexOf('@');
  if (at < 0) return s;
  const colon = s.lastIndexOf(':');
  if (colon < at) return s.slice(0, at + 1) + s.slice(at + 1).trim();
  return s.slice(0, at + 1) + s.slice(at + 1, colon).trim() + ':' + s.slice(colon + 1).trim();
}

function chartColors(n) {
  const base = ['#2563eb','#16a34a','#d97706','#dc2626','#a855f7','#0891b2','#db2777','#65a30d','#ea580c','#0284c7','#7c3aed','#059669'];
  const out = [];
  for (let i = 0; i < n; i++) out.push(base[i % base.length]);
  return out;
}
