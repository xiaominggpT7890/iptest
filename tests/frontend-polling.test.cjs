const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

function fixture() {
  const requests = [];
  const ticks = [];
  const context = vm.createContext({
    fetch(url) {
      return new Promise(resolve => requests.push({
        url,
        respond(body, status = 200) {
          resolve({ ok: status < 400, status, json: async () => body });
        },
      }));
    },
    setInterval() { return 1; },
    clearInterval() {},
  });
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../internal/api/static/app.js'), 'utf8'), context);
  const app = context.iptestApp();
  const messages = [];
  app.notify = message => messages.push(message);
  app.$nextTick = callback => ticks.push(callback);
  app.refreshIcons = () => {};
  app.drawCharts = () => {};
  return { app, requests, messages, ticks };
}

test('late task list cannot roll back newer counters', async () => {
  const { app, requests } = fixture();
  const older = app.loadTasks();
  const newer = app.loadTasks();
  requests[1].respond([{ id: 643, total_requests: 200 }]);
  await newer;
  requests[0].respond([{ id: 643, total_requests: 100 }]);
  await older;
  assert.equal(app.tasks[0].total_requests, 200);
});

test('automatic list polling does not overlap slow requests', async () => {
  const { app, requests } = fixture();
  const first = app.loadTasks(true);
  await app.loadTasks(true);
  assert.equal(requests.length, 1);
  requests[0].respond([]);
  await first;
  assert.equal(app.tasksLoading, false);
});

test('a late source report cannot replace the copied task report', async () => {
  const { app, requests, ticks } = fixture();
  const older = app.openReport(19);
  const newer = app.openReport(643);
  requests[2].respond({ task: { id: 643, total_requests: 200 } });
  requests[3].respond([]);
  await newer;
  requests[0].respond({ task: { id: 19, total_requests: 100 } });
  requests[1].respond([]);
  await older;
  assert.equal(app.report.task.id, 643);
  assert.equal(ticks.length, 1);
});

test('late responses and queued charts do not update a closed report', async () => {
  const { app, requests, ticks } = fixture();
  let draws = 0;
  app.drawCharts = () => draws++;
  const loaded = app.openReport(643);
  requests[0].respond({ task: { id: 643 } });
  requests[1].respond([]);
  await loaded;
  app.view = 'list';
  ticks.forEach(callback => callback());
  assert.equal(draws, 0);
  const previous = app.report;
  const late = app.loadReport(643);
  requests[2].respond({ task: { id: 643, total_requests: 300 } });
  requests[3].respond([]);
  await late;
  assert.equal(app.report, previous);
});

test('a report for the wrong task is rejected', async () => {
  const { app, requests, messages } = fixture();
  const loaded = app.openReport(643);
  requests[0].respond({ task: { id: 19 } });
  requests[1].respond([]);
  await loaded;
  assert.equal(app.report, null);
  assert.ok(messages.some(message => message.includes('报表任务不匹配')));
});

test('duplicate clicks send a single lifecycle request', async () => {
  const { app, requests, messages } = fixture();
  const first = app.act(643, 'start');
  await app.act(643, 'start');
  assert.equal(requests.length, 1);
  requests[0].respond({ error: 'already running' }, 400);
  await first;
  assert.equal(app.pendingActions[643], undefined);
  assert.deepEqual(messages, ['already running']);
});

test('invalid list responses retain the last valid data', async () => {
  const { app, requests } = fixture();
  app.tasks = [{ id: 643, total_requests: 200 }];
  const loaded = app.loadTasks();
  requests[0].respond({ error: 'database unavailable' }, 500);
  await loaded;
  assert.equal(app.tasks[0].total_requests, 200);
});

test('failed automatic start is not reported as running', async () => {
  const { app, requests, messages } = fixture();
  app.form.proxy.template = 'test-{num}:password@127.0.0.1:1080';
  const created = app.createTask(true);
  requests[0].respond({ id: 643 });
  await new Promise(resolve => setImmediate(resolve));
  requests[1].respond({ error: 'startup failed' }, 400);
  await new Promise(resolve => setImmediate(resolve));
  requests[2].respond([{ id: 643, state: 'created' }]);
  await created;
  assert.equal(app.tasks[0].state, 'created');
  assert.ok(messages.some(message => message.includes('启动失败: startup failed')));
  assert.ok(messages.every(message => !message.includes('已开始任务')));
});

test('saving trims endpoint whitespace and preserves credential whitespace', () => {
  const { app } = fixture();
  app.form.proxy.template = 'user-{num}:p a@ss@ gw1.example.com :\u00a015015 ';
  const config = app.toConfig();
  assert.equal(config.proxy.template, 'user-{num}:p a@ss@gw1.example.com:15015');
  assert.equal(app.form.proxy.template, config.proxy.template);
  app.form.proxy.template = 'socks5://user-{num}:pass@ [2001:db8::1] : 15015';
  assert.equal(app.toConfig().proxy.template, 'socks5://user-{num}:pass@[2001:db8::1]:15015');
});

test('random is default and saving preserves the selected sequential mode', () => {
  const { app } = fixture();
  assert.equal(app.form.sessid_range.mode, 'random');
  assert.equal(app.toConfig().sessid_range.mode, 'random');
  app.form.sessid_range = { min: 10, max: 20, mode: 'sequential' };
  const range = app.toConfig().sessid_range;
  assert.equal(range.min, 10);
  assert.equal(range.max, 20);
  assert.equal(range.mode, 'sequential');
});

test('editing and cloning legacy tasks preserve random mode without sharing range objects', async () => {
  const { app } = fixture();
  app.tasks = [{ id: 644, name: 'legacy', state: 'paused', config: { sessid_range: { min: 10, max: 20 } } }];
  await app.editTask(644);
  assert.equal(app.form.sessid_range.mode, 'random');
  app.form.sessid_range.min = 11;
  assert.equal(app.tasks[0].config.sessid_range.min, 10);
  await app.cloneTask(644);
  assert.equal(app.form.sessid_range.mode, 'random');
  assert.equal(app.form._editingId, null);
  app.form.sessid_range.max = 25;
  assert.equal(app.tasks[0].config.sessid_range.max, 20);
});

test('editing and cloning sequential tasks retain the selected mode', async () => {
  const { app } = fixture();
  app.tasks = [{ id: 644, name: 'sequential', state: 'paused', config: { sessid_range: { min: 10, max: 20, mode: 'sequential' } } }];
  await app.editTask(644);
  assert.equal(app.form.sessid_range.mode, 'sequential');
  await app.cloneTask(644);
  assert.equal(app.form.sessid_range.mode, 'sequential');
});

test('report shows sequential next number and wraps an end boundary to minimum', () => {
  const { app } = fixture();
  const task = { id: 644, config: { sessid_range: { min: 10, max: 20, mode: 'sequential' } } };
  assert.equal(app.nextSessidLabel(task), '10');
  task.sweep_next_sessid = 17;
  assert.equal(app.nextSessidLabel(task), '17');
  task.sweep_next_sessid = 21;
  assert.equal(app.nextSessidLabel(task), '10');
  task.config.sessid_range.mode = 'random';
  assert.equal(app.nextSessidLabel(task), '-');
});
