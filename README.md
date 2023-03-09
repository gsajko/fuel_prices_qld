# Fuel Prices QLD project

This project is a simple web app that displays the historical fuel prices in Queensland, Australia.

## Data

Data is sourced from my other project [QLD_fuel_scraping](https://github.com/gsajko/QLD_fuel_scraping)


## Install

### Poetry

First you need to install `poetry`. For this project I use version `1.4.0`

https://python-poetry.org/docs/#installation

Remeber to add poetry to your path.

`export PATH="/home/[user]/.local/bin:$PATH"`

### Init submodule

???

### 

## App overview

```mermaid
flowchart LR

d1[csv monthly]
d2[csv daily]

subgraph ingest
d1 & d2 --> parquet --> gcs[Google Cloud Storage]
end

gcs --> gbq[Google BigQuery]

subgraph transform
gbq --> dbt[dbt]
end

subgraph serve
dbt --> serv[web app]
end

```

### todo
- [ ] ingest data from csv to GCS in parquet format
- [ ] create external tables in BigQuery
- [ ] create dbt models
- [ ] deploy web app

---
- [ ] use terraform to create GCP resources
