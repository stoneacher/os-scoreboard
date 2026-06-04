# TU Graz OS-scoreboard-monitor

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-2.7-22ADF6?logo=influxdb&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-11.6-F46800?logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

Scrapes the public [TU Graz Tacos/Sweb OS scoreboard](https://sweb.student.isec.tugraz.at/), normalizes the standings, stores time-series snapshots in InfluxDB 2 OSS, and exposes the data to Grafana.

This project is deployed on my vps and in use since SS 2026.

---
### Public source-available repository
Use, copying, modification, or redistribution requires prior written permission.
