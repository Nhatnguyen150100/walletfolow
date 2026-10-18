// =====================================================================
// Load test lần đầu (roadmap 1.5b) — POST /api/v1/transactions/transfer qua gateway.
// Chạy qua scripts/load/run.sh, không gọi tay (cần dữ liệu từ scripts/load/seed.sh).
//
// Mô hình MỞ (constant-arrival-rate): k6 bắn đúng R request/giây bất kể hệ thống trả lời nhanh hay
// chậm — giống người dùng thật. Mô hình đóng (N VU lặp) tự giảm tải khi hệ thống chậm và che mất
// điểm bão hoà. Mỗi mức TPS là một scenario riêng, chạy nối tiếp => số liệu tách theo từng mức.
//
// MODE=cold: người gửi ngẫu nhiên -> người nhận ngẫu nhiên (khoá rải đều).
// MODE=hot : người gửi ngẫu nhiên -> MỘT ví merchant (mọi bút toán tranh khoá tài khoản nhận).
//
// Mỗi lệnh mang note "load:<RUN_ID>:<scenario>" để scripts/load/run.sh tính độ trễ END-TO-END (nhận
// lệnh -> COMPLETED) theo từng mức bằng SQL — không cần poll GET /transactions/{id}, vốn ăn vào cùng
// hạn mức 10 rps/user với POST và làm méo chính thứ đang đo.
// =====================================================================
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import exec from 'k6/execution';

const GATEWAY = __ENV.GATEWAY || 'http://api-gateway:8080';
const MODE = __ENV.MODE || 'cold';
const RUN_ID = __ENV.RUN_ID || `${Date.now()}`;
const LEVELS = (__ENV.LEVELS || '5,10,20,40,80').split(',').map(Number);
const STAGE_SEC = Number(__ENV.STAGE_SEC || 60);
const GAP_SEC = Number(__ENV.GAP_SEC || 10);
const AMOUNT = Number(__ENV.AMOUNT || 1000);
const CURRENCY = __ENV.CURRENCY || 'VND';

if (!['cold', 'hot'].includes(MODE)) {
  throw new Error(`MODE phải là cold|hot, nhận ${MODE}`);
}

const DATA = JSON.parse(open('/scripts/.data/users.json'));

const transferOk = new Rate('transfer_accepted');
const rateLimited = new Counter('transfer_rate_limited');

// Mỗi mức: scenario riêng, bắt đầu sau mức trước + khoảng nghỉ cho hàng đợi kịp thở
const scenarios = {};
const thresholds = {};
LEVELS.forEach((rate, i) => {
  const name = `${MODE}_${String(rate).padStart(3, '0')}`;
  scenarios[name] = {
    executor: 'constant-arrival-rate',
    rate,
    timeUnit: '1s',
    duration: `${STAGE_SEC}s`,
    startTime: `${i * (STAGE_SEC + GAP_SEC)}s`,
    preAllocatedVUs: Math.max(5, rate * 2),
    maxVUs: Math.max(20, rate * 10),
    exec: 'transfer',
  };
  // Threshold luôn đúng — chỉ để k6 xuất sub-metric theo từng scenario vào summary
  thresholds[`http_req_duration{scenario:${name},name:transfer}`] = ['max>=0'];
  thresholds[`transfer_accepted{scenario:${name}}`] = ['rate>=0'];
  thresholds[`transfer_rate_limited{scenario:${name}}`] = ['count>=0'];
  thresholds[`dropped_iterations{scenario:${name}}`] = ['count>=0'];
});

export const options = {
  scenarios,
  thresholds,
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max', 'count'],
  setupTimeout: '300s',
  // Không gửi số liệu ẩn danh của k6 ra ngoài
  noUsageStats: true,
};

// Đăng nhập mọi người gửi một lần. /api/auth bị giới hạn 5 rps/IP (burst 10) => đi chậm, gặp 429
// thì chờ. Token sống 15 phút: toàn bộ các mức phải chạy xong trong khoảng đó.
export function setup() {
  const users = DATA.users.map((u) => {
    for (let attempt = 0; attempt < 30; attempt++) {
      const res = http.post(`${GATEWAY}/api/auth/login`,
        JSON.stringify({ email: u.email, password: u.password }),
        { headers: { 'Content-Type': 'application/json' }, tags: { name: 'login' } });
      if (res.status === 200) {
        sleep(0.25);
        return { token: res.json('data.accessToken'), walletId: u.walletId };
      }
      if (res.status !== 429) {
        throw new Error(`Đăng nhập ${u.email}: HTTP ${res.status} ${res.body}`);
      }
      sleep(1);
    }
    throw new Error(`Đăng nhập ${u.email}: vẫn 429 sau 30 lần`);
  });
  return { users, merchant: DATA.merchant };
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export function transfer(data) {
  const sender = pick(data.users);
  let to = data.merchant;
  if (MODE === 'cold') {
    do {
      to = pick(data.users).walletId;
    } while (to === sender.walletId);
  }
  const scenario = exec.scenario.name;
  const body = JSON.stringify({
    fromWalletId: sender.walletId,
    toWalletId: to,
    amountMinor: AMOUNT,
    currency: CURRENCY,
    note: `load:${RUN_ID}:${scenario}`,
  });
  // Key duy nhất cho mỗi lệnh; bị 429 thì gửi lại CÙNG key — đúng cách client phải làm
  const key = `load-${RUN_ID}-${exec.vu.idInTest}-${exec.vu.iterationInScenario}-${scenario}`;
  const params = {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${sender.token}`,
      'Idempotency-Key': key,
    },
    tags: { name: 'transfer' },
  };
  let res = http.post(`${GATEWAY}/api/v1/transactions/transfer`, body, params);
  for (let attempt = 0; res.status === 429 && attempt < 5; attempt++) {
    rateLimited.add(1);
    sleep(1);
    res = http.post(`${GATEWAY}/api/v1/transactions/transfer`, body, params);
  }
  const ok = check(res, { 'transfer 202': (r) => r.status === 202 });
  transferOk.add(ok);
}

export function handleSummary(data) {
  // setup_data chứa bearer token của mọi người gửi — không để nó nằm trong file summary
  delete data.setup_data;
  return {
    [`/scripts/.out/summary-${RUN_ID}.json`]: JSON.stringify(data, null, 2),
    stdout: renderTable(data),
  };
}

// Bảng gọn theo từng mức — số chi tiết nằm trong file JSON
function renderTable(data) {
  const m = data.metrics;
  const fmt = (v) => (v === undefined ? '-' : Number(v).toFixed(1));
  const rows = Object.keys(scenarios).map((name) => {
    const d = m[`http_req_duration{scenario:${name},name:transfer}`];
    const okRate = m[`transfer_accepted{scenario:${name}}`];
    const limited = m[`transfer_rate_limited{scenario:${name}}`];
    const dropped = m[`dropped_iterations{scenario:${name}}`];
    const v = d ? d.values : {};
    return `| ${name} | ${scenarios[name].rate} | ${v.count ?? 0} | ${fmt(v['p(50)'])} | `
      + `${fmt(v['p(95)'])} | ${fmt(v['p(99)'])} | ${fmt(v.max)} | `
      + `${okRate ? (okRate.values.rate * 100).toFixed(2) : '-'}% | `
      + `${limited ? limited.values.count : 0} | ${dropped ? dropped.values.count : 0} |`;
  });
  return [
    '',
    `RUN_ID=${RUN_ID} MODE=${MODE}`,
    '| scenario | TPS đặt | request | p50 ms | p95 ms | p99 ms | max ms | 202 | 429 | bỏ lỡ |',
    '|---|---|---|---|---|---|---|---|---|---|',
    ...rows,
    '',
  ].join('\n');
}
