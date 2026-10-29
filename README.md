# TU Graz OS-scoreboard-monitor

![Python](https://flat.badgen.net/badge/Python/3.12/3776AB)
![InfluxDB](https://flat.badgen.net/badge/InfluxDB/2.7/22ADF6)
![Grafana](https://flat.badgen.net/badge/Grafana/13.3/F46800)

Scrapes the public [TU Graz Tacos/Sweb OS scoreboard](https://sweb.student.isec.tugraz.at/), normalizes the standings, stores time-series snapshots in InfluxDB 2 OSS, and exposes the data to Grafana.

This project is deployed on my vps and in use since SS 2026.

**Check out the latest snapshot of [last summer semester (SS2026)](https://os-scoreboard.sest-it.at/dashboard/snapshot/d2Di7urjgQqpS570xMd7oJ6HJgQwWckU?from=2026-04-21T09:01:20.291Z&to=2026-07-21T21:40:19.661Z&timezone=browser&var-bucket=scoreboard-ss2026&var-team_id=$__all&var-submission_type=$__all&refresh=5m):**

![Screenshot Summer Semester 2026](public/screenshot_ss2026.png)

This project was only intended to help me and my team to monitor our progress during the semester, but I decided to keep it running and make it available to any student that is currently taking OS\*. I am currently working on creating a public read-only Grafane Organization that will allow any student to view the dashboard without having to use credentials. This however needs quite a big amount of work if I want to keep it secure, so bear with me.

---
### Public source-available repository
Use, copying, modification, or redistribution requires prior written permission.


---
\* *if you are currently enrolled in OS and would like to use the dashboard for the current semester, write me an [email](mailto:sebastian@sest-it.at) and I'll provide you with credentials your team can use :)*
