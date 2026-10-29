# TU Graz OS-scoreboard-monitor

![Python](https://flat.badgen.net/badge/Python/3.12/3776AB)
![InfluxDB](https://flat.badgen.net/badge/InfluxDB/2.7/22ADF6)
![Grafana](https://flat.badgen.net/badge/Grafana/11.6/F46800)
![Docker](https://flat.badgen.net/badge/Docker/Compose/2496ED?icon=docker)

Scrapes the public [TU Graz Tacos/Sweb OS scoreboard](https://sweb.student.isec.tugraz.at/), normalizes the standings, stores time-series snapshots in InfluxDB 2 OSS, and exposes the data to Grafana.

This project is deployed on my vps and in use since SS 2026.

### Team name history

The scoreboard dashboard includes **Team Name Summary** (distinct name count and
all observed names per team ID) and **Team Name History** (first and last observed
timestamps per name). Select a `teamID`, such as `B4`, to inspect its aliases.
Both tables use all retained history in the selected semester bucket, regardless
of the dashboard time range, and respect the `submission_type` filter.

Names come from the stored scoreboard display labels. ID-only labels are excluded
from both tables and the distinct count when another name exists for that team
within the selected semester and submission type filters. Teams with no other
observed name keep their ID-only label and have a distinct name count of zero.
Reusing a previous name does not increase the distinct count. First/last observed
timestamps describe sightings, including gaps, rather than exact rename times.
Historical names already in InfluxDB are included; no backfill is needed.

To update an existing Grafana installation, import
`grafana/dashboards/sweb-scoreboard.json` and overwrite the existing scoreboard
dashboard. Docker Compose currently provisions the datasource only; pulling this
file and restarting containers does not update the dashboard automatically.

---
### Public source-available repository
Use, copying, modification, or redistribution requires prior written permission.
