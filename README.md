# TU Graz OS-scoreboard-monitor

![Python](https://flat.badgen.net/badge/Python/3.12/3776AB)
![InfluxDB](https://flat.badgen.net/badge/InfluxDB/2.7/22ADF6)
![Grafana](https://flat.badgen.net/badge/Grafana/11.6/F46800)
![Docker](https://flat.badgen.net/badge/Docker/Compose/2496ED?icon=docker)

Scrapes the public [TU Graz Tacos/Sweb OS scoreboard](https://sweb.student.isec.tugraz.at/), normalizes the standings, stores time-series snapshots in InfluxDB 2 OSS, and exposes the data to Grafana.

This project is deployed on my vps and in use since SS 2026.

---
### Public source-available repository
Use, copying, modification, or redistribution requires prior written permission.
