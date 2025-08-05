import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    contacts: {
      executor: 'constant-arrival-rate',

      duration: '30s',

      rate: 2500,

      timeUnit: '1s',

      preAllocatedVUs: 1000,

      maxVUs: 3000,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
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