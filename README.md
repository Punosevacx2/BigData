# Spatial Analysis of Urban Infrastructure Using Large-Scale OpenStreetMap Data

> **Prostorna Analiza Urbane Infrastrukture Koriscenjem OpenStreetMap Podataka Velikog Obima**
> Master's thesis project — Big Data course

## Overview

This project implements a scalable Big Data pipeline for analyzing urban infrastructure patterns from OpenStreetMap (OSM) data. Raw OSM XML is parsed, converted to columnar storage format, and analyzed using distributed computing to extract insights about highways, amenities, buildings, transport networks, and land use.

## Architecture

```
OSM XML  -->  TSV  -->  Parquet (HDFS)  -->  Spark Analysis  -->  CSV Results
```

| Stage | Script | Description |
|---|---|---|
| 1. Parse | `src/osm_to_tsv.py` | Streams OSM XML, extracts node/way tags into TSV |
| 2. Convert | `src/tsv_to_parquet.py` | Converts TSV to Parquet via PySpark with type casting |
| 3. Analyze | `src/analyze.py` | Distributed aggregations over Parquet with PySpark |

## Tech Stack

- **Apache Spark 3.3.0** — distributed data processing (PySpark)
- **Apache Hadoop 3.2.1** — HDFS storage + YARN resource management
- **Docker Compose** — local cluster orchestration (Spark master/workers + full Hadoop stack)
- **Python 3** — pipeline scripting

## Project Structure

```
BigData/
├── src/
│   ├── osm_to_tsv.py        # Stage 1: OSM XML -> TSV
│   ├── tsv_to_parquet.py    # Stage 2: TSV -> Parquet (PySpark)
│   └── analyze.py           # Stage 3: Distributed analysis (PySpark)
├── docker-compose.yml       # Spark + Hadoop cluster definition
├── hadoop.env               # Hadoop/YARN configuration
└── data/                    # Input/output data (gitignored)
    └── processed/
```

## Prerequisites

- Docker & Docker Compose
- Python 3.8+
- PySpark (`pip install pyspark`)
- OSM data file (`.osm` XML) — download from [OpenStreetMap](https://www.openstreetmap.org/export) or [Geofabrik](https://download.geofabrik.de/)

## Setup & Usage

### 1. Start the cluster

```bash
# Create the shared Docker network (one-time setup)
docker network create bde

# Start all services (Spark master/workers + Hadoop stack)
docker compose up -d
```

**Web UIs:**

| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark Worker 2 | http://localhost:8082 |
| HDFS NameNode | http://localhost:9870 |

### 2. Run the pipeline

```bash
# Stage 1: Parse OSM XML to TSV
python src/osm_to_tsv.py data/processed/region.osm data/processed/osm.tsv

# Stage 2: Convert TSV to Parquet
python src/tsv_to_parquet.py data/processed/osm.tsv data/processed/osm.parquet

# Stage 3: Run analysis
python src/analyze.py data/processed/osm.parquet results/
```

To run against HDFS instead of local filesystem, pass HDFS paths:

```bash
python src/analyze.py hdfs://namenode:9000/user/osm/osm.parquet hdfs://namenode:9000/user/osm/results
```

### 3. Stop the cluster

```bash
docker compose down
```

## Analysis Output

The pipeline produces CSV files in the output directory:

| File | Description |
|---|---|
| `top_highway/` | Top 30 highway types by frequency |
| `top_amenity/` | Top 30 amenity types by frequency |
| `buildings_count.txt` | Total number of tagged buildings |
| `top_oneway/` | One-way street distribution |
| `top_lanes/` | Lane count distribution |
| `top_maxspeed/` | Speed limit distribution |
| `top_surface/` | Road surface types |
| `top_bridge/` | Bridge tag distribution |
| `top_railway/` | Railway infrastructure types |
| `top_public_transport/` | Public transport stop types |
| `top_landuse/` | Land use categories |

## Data Schema

The Parquet dataset has the following schema:

| Column | Type | Description |
|---|---|---|
| `id` | `long` | OSM element ID |
| `type` | `string` | Element type (`node` or `way`) |
| `lat` | `double` | Latitude (nodes only) |
| `lon` | `double` | Longitude (nodes only) |
| `k` | `string` | Tag key |
| `v` | `string` | Tag value |

## Infrastructure Services

The Docker Compose stack includes:

- **spark-master** — Spark standalone master (`spark://spark-master:7077`)
- **spark-worker-1/2** — Two Spark workers
- **namenode** — HDFS NameNode
- **datanode** — HDFS DataNode
- **resourcemanager** — YARN ResourceManager
- **nodemanager** — YARN NodeManager
- **historyserver** — YARN Job History Server
