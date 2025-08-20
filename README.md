
## Результаты

Целевая метрика p(95) < 500ms при нагрузочном тестировании RPS = 2500 в течение 60s


|     | Architecture                            | p(95)                              | error rate (timeout=5s) | total requests (60s) | kafka lag (peak) | kafka lag solve |
| --- | --------------------------------------- | ---------------------------------- | ----------------------- | -------------------- | ---------------- | --------------- |
| 1   | python + kafka + clickhouse             | $${\color{red}5.51\space s}$$      | 60.86%                  | 50533                | 300              | 1:15            |
| 2   | python + 3 kafka + clickhouse           | $${\color{red}5.54\space s}$$      | 38.79%                  | 55643                | 1115             | 1:00            |
| 3   | nginx + 3 python + kafka + clickhouse   | $${\color{orange}47.26\space ms}$$ | 0.00%                   | 150000               | 48600            | 1:30            |
| 4   | nginx + 3 python + 3 kafka + clickhouse | $${\color{orange}49.2\space ms}$$  | 0.00%                   | 150000               | 51300            | 1:30            |
| 5   | go + kafka + clickhouse                 | $${\color{green}2.16\space ms}$$   | 0.00%                   | 150000               | 48600            | 1:30            |
| 6   | go + 3 kafka + clickhouse               | $${\color{green}5.68 \space ms}$$  | 0.00%                   | 150000               | 50000            | 1:45            |
| 7   | nginx + 3 go + 3 kafka + clickhouse     | $${\color{green}1.14\space ms}$$   | 0.00%                   | 150000               | 41300            | 1:15            |
 
