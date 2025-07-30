import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// export const options = {
//     stages: [
//         { duration: '30s', target: 50 },  // постепенно увеличиваем нагрузку до 50 пользователей
//         { duration: '1m', target: 50 },   // держим нагрузку на уровне 50 пользователей
//         { duration: '30s', target: 100 }, // увеличиваем до 100 пользователей
//         { duration: '1m', target: 100 },  // держим нагрузку на уровне 100 пользователей
//         { duration: '30s', target: 0 },    // постепенно снижаем нагрузку
//     ],
//     thresholds: {
//         http_req_duration: ['p(95)<500'], // 95% запросов должны выполняться быстрее 500ms
//     },  
// };

export const options = {
    scenarios: {
      stress_test: {
        executor: 'ramping-arrival-rate',
        startRate: 500, // Начальный RPS
        timeUnit: '1s',
        stages: [
          { duration: '30s', target: 2500 }, 
          { duration: '1m', target: 2500 },
          { duration: '30s', target: 0 },
        ],
        preAllocatedVUs: 10,
        maxVUs: 50,
      },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'], // Менее 1% ошибок
        http_req_duration: ['p(95)<500'], // 95% запросов должны отвечать быстрее 500 мс
    },
  };

const BASE_URL = 'http://localhost:8000';

export default function () {

    const eventTypes = ['play', 'pause', 'skip', 'like', 'dislike', 'add_to_playlist', 'remove_from_playlist'];
    const randomEventType = eventTypes[Math.floor(Math.random() * eventTypes.length)];
    
    const trackEvent = {
        action_id: uuidv4(),
        action_time: new Date().toISOString(),
        user_id: uuidv4(),
        action_type: randomEventType,
        track_id: uuidv4(),
        recommended: Math.random() > 0.5,
        playlist_id: Math.random() > 0.7 ? uuidv4() : null,
        duration: randomEventType === 'play' || randomEventType === 'pause' ? Math.floor(Math.random() * 300) : null
    };

    const headers = {
        'Content-Type': 'application/json',
    };

    const res = http.post(
        `${BASE_URL}/tracks`,
        JSON.stringify(trackEvent),
        { headers }
    );

    check(res, {
        'status is 200': (r) => r.status === 200,
    });

}