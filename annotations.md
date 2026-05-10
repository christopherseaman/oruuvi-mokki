# Oruuvi-Mokki: Manual Annotations

Hand-recorded events for the airflow experiment. Append-only log; one row per
event. To be migrated into Grafana annotations once an input/save flow exists.

Columns:
- **local** — Pacific wall-clock time (PDT = UTC−7 in May)
- **iso** — ISO 8601 with offset (canonical, unambiguous)
- **kind** — `start`, `end`, or `point`
- **tag** — short identifier; pair `start` and `end` rows by tag for ranges
- **note** — free text

| local                 | iso                          | kind  | tag        | note                     |
| --------------------- | ---------------------------- | ----- | ---------- | ------------------------ |
| 2026-05-09 04:15 PDT  | 2026-05-09T04:15:00-07:00    | start | fan-test-1 | Sensor in place, fan on  |
| 2026-05-09 13:15 PDT  | 2026-05-09T13:15:00-07:00    | end   | fan-test-1 | Fan off                  |
