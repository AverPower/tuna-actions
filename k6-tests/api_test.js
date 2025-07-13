import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 100 },  // Плавный рост
    { duration: '1m', target: 500 },   // Пиковая нагрузка
    { duration: '20s', target: 0 },    // Завершение
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],  // 95% запросов должны быть быстрее 500мс
    http_req_failed: ['rate<0.01'],    // Менее 1% ошибок
  },
};

export default function () {
  const res = http.get('http://localhost:8000/tracks/popular?days=7');
  check(res, {
    'status is 200': (r) => r.status === 200,
  });
  sleep(1);
}