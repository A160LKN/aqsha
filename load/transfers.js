// Нагрузочный тест по gRPC: клиенты одновременно переводят деньги между 20 счетами.
// После нагрузки — проверка целостности: сумма всех балансов не изменилась, минусов нет.
// Запуск: k6 run load/transfers.js (адреса — PAYMENTS_ADDR, LEDGER_ADDR, ключ — API_KEY)
import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const PAYMENTS = __ENV.PAYMENTS_ADDR || 'localhost:9090';
const LEDGER = __ENV.LEDGER_ADDR || 'localhost:9091';
const KEY = __ENV.API_KEY || 'demo-key-change-me';
const ACCOUNTS = 20;
const START = 1_000_000_00; // 1 млн ₸ на каждом счёте
const AUTH = { metadata: { 'x-api-key': KEY } }; // сервис счетов тоже требует ключ

const client = new grpc.Client();
client.load(['../proto'], 'aqsha/payments/v1/payments.proto', 'aqsha/ledger/v1/ledger.proto');
const integrity = new Counter('integrity_failures');

export const options = {
  scenarios: {
    transfers: { executor: 'ramping-vus', stages: [
      { duration: '10s', target: 20 },
      { duration: '30s', target: 20 },
      { duration: '5s', target: 0 },
    ] },
  },
  thresholds: {
    'grpc_req_duration{method:CreateTransfer}': ['p(95)<300', 'p(99)<800'],
    checks: ['rate>0.99'],
    integrity_failures: ['count==0'],
  },
};

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export function setup() {
  client.connect(LEDGER, { plaintext: true });
  const ids = [];
  for (let i = 0; i < ACCOUNTS; i++) {
    const acc = client.invoke('aqsha.ledger.v1.LedgerService/OpenAccount', { owner: `load-${i}` }, AUTH);
    const id = acc.message.account.id;
    client.invoke('aqsha.ledger.v1.LedgerService/Deposit', { accountId: id, amountTiyn: START, depositId: uuid() }, AUTH);
    ids.push(id);
  }
  client.close();
  return { ids };
}

let connected = false;
export default function (data) {
  if (!connected) {
    client.connect(PAYMENTS, { plaintext: true });
    connected = true;
  }
  const from = data.ids[Math.floor(Math.random() * ACCOUNTS)];
  let to = from;
  while (to === from) to = data.ids[Math.floor(Math.random() * ACCOUNTS)];
  const res = client.invoke('aqsha.payments.v1.PaymentService/CreateTransfer', {
    idempotencyKey: uuid(), fromAccount: from, toAccount: to, amountTiyn: 1 + Math.floor(Math.random() * 500_00),
  }, { metadata: { 'x-api-key': KEY, 'x-request-id': `k6-${exec.vu.idInTest}-${exec.vu.iterationInScenario}` } });
  check(res, { 'перевод принят': (r) => r && r.status === grpc.StatusOK });
  sleep(0.05);
}

export function teardown(data) {
  sleep(10); // даём Kafka и сервису счетов догнать очередь
  client.connect(LEDGER, { plaintext: true });
  let total = 0;
  for (const id of data.ids) {
    const b = Number(client.invoke('aqsha.ledger.v1.LedgerService/GetAccount', { id }, AUTH).message.account.balanceTiyn);
    if (b < 0) integrity.add(1);
    total += b;
  }
  client.close();
  const expected = ACCOUNTS * START;
  if (total !== expected) {
    integrity.add(1);
    console.error(`деньги не сошлись: ${total} вместо ${expected}`);
  } else {
    console.log(`целостность в порядке: сумма балансов ${total} тиынов`);
  }
}
