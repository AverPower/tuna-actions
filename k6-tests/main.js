import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomItem, randomInt } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import endpoints from './config/endpoints.json';

// 1. Загрузка тестовых данных (CSV/JSON)
const testData = JSON.parse(open('./data/test_data.json'));

// 2. Конфиг теста
export const options = {
  stages: [
    { duration: '2m', target: 50 },   // Разогрев
    { duration: '5m', target: 200 },  // Пиковая нагрузка
    { duration: '1m', target: 0 },    // Завершение
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01']
  }
};

// 3. Инициализация (например, получение токена)
export function setup() {
  const authRes = http.post('https://api.example.com/auth', {
    username: __ENV.API_USER,
    password: __ENV.API_PASS
  });
  return { token: authRes.json('access_token') };
}

// 4. Основная логика теста
export default function (data) {
  const endpoint = randomItem(endpoints.endpoints);
  const url = `https://api.example.com${endpoint.path}`;
  const params = {
    headers: {
      'Authorization': `Bearer ${data.token}`,
      'Content-Type': 'application/json'
    }
  };

  let res;
  switch (endpoint.method) {
    case 'GET':
      // Динамические query-параметры
      const query = new URLSearchParams();
      for (const [key, values] of Object.entries(endpoint.query_params)) {
        query.set(key, randomItem(values));
      }
      res = http.get(`${url}?${query.toString()}`, params);
      break;

    case 'POST':
      // Генерация тела запроса
      let body = endpoint.body;
      if (typeof body === 'object') {
        body = JSON.stringify(body)
          .replace(/{{randomInt\((.*?)\)}}/g, (_, args) => randomInt(...args.split(',').map(Number)))
          .replace(/{{randomString\((.*?)\)}}/g, (_, len) => randomString(Number(len)));
      }
      res = http.post(url, body, params);
      break;
  }

  // Проверка результата
  check(res, {
    [`${endpoint.name} status 200`]: (r) => r.status === 200,
    [`${endpoint.name} valid response`]: (r) => r.json() !== null
  });

  sleep(randomInt(1, 3)); // Имитация задержки пользователя
}