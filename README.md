cat > /tmp/README.md << 'ENDOFREADME'
# 🏎️ Formula 1 Data Analysis Platform — Incremental Load

An end-to-end data engineering project built on **Azure Databricks** and **Azure Data Lake Storage Gen2 (ADLS Gen2)**, implementing the **Medallion Architecture** (Bronze → Silver → Gold) with **incremental batch processing** for Formula 1 racing data. The project delivers insights on **Driver Standings** and **Constructor Standings** through analysis notebooks and interactive dashboards, governed by **Unity Catalog**.

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Unity Catalog Setup](#unity-catalog-setup)
- [Medallion Architecture](#medallion-architecture)
- [Incremental Load & Batch Orchestration](#incremental-load--batch-orchestration)
- [Helper Modules](#helper-modules)
- [Notebooks](#notebooks)
- [Dashboards](#dashboards)
- [How to Run](#how-to-run)

---

## 🏗️ Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│              Azure Data Lake Storage Gen2 (ADLS Gen2)            │
│         /Volumes/formula1_incr/landing/files/<batch_id>/         │
└────────────────────────┬─────────────────────────────────────────┘
                         │  Unity Catalog Volume
                         ▼
          ┌──────────────────────────────┐
          │   formula1_incr.control      │
          │   batch_control table        │
          │   (tracks batch status)      │
          └──────────────┬───────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
  ┌────────────┐  ┌────────────┐  ┌────────────┐
  │  02-bronze │  │  03-silver │  │  04-gold   │
  │            │  │            │  │            │
  │ Raw ingest │─►│ Cleanse &  │─►│ Aggregate  │
  │ + batch_id │  │  Upsert    │  │ Dimensions │
  │ partition  │  │  (MERGE)   │  │  & Facts   │
  └────────────┘  └────────────┘  └─────┬──────┘
                                        │
                          ┌─────────────┼──────────────┐
                          ▼             ▼              ▼
                   ┌───────────┐ ┌───────────┐ ┌──────────────┐
                   │  Driver   │ │Constructor│ │  Databricks  │
                   │ Standings │ │ Standings │ │  Dashboards  │
                   │   View    │ │   View    │ │              │
                   └───────────┘ └───────────┘ └──────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Cloud Storage** | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| **Compute** | Azure Databricks |
| **Storage Format** | Delta Lake |
| **Data Governance** | Unity Catalog (Volumes, Catalog, Schema) |
| **Language** | PySpark, Spark SQL, Python |
| **Orchestration** | Databricks Workflows + Control Table pattern |
| **Visualization** | Databricks Dashboards |
| **Version Control** | GitHub (via Databricks Git Integration) |
| **Architecture** | Medallion Architecture — Bronze / Silver / Gold |
| **Load Strategy** | Incremental Batch Load with `batch_id` tracking |

---

## 📁 Project Structure

```
formula1-project-incremental-load/
│
├── 📁 00-Common/                         # Shared helper modules (%run)
│   ├── 01.environment-config.py          # Catalog, schema, volume path config
│   ├── 02.bronze-helpers.py              # add_ingestion_metadata(), write_to_bronze()
│   ├── 03.silver_helpers.py              # write_to_silver() — MERGE upsert logic
│   └── 04.gold_helpers.py                # Gold layer utility functions
│
├── 📁 01-setup/                          # One-time environment setup
│   ├── 01.Setup Project Environment.sql  # Creates catalogs, schemas, volumes
│   └── 02.Setup Batch Events.py          # Seeds batch event reference data
│
├── 📁 02-bronze/                         # Raw ingestion notebooks
│   ├── 01.Ingest Circuits file.py
│   ├── 02.Ingest races file.py
│   ├── 03.Ingest Constructors file.py
│   ├── 04.Ingest Drivers file.py
│   ├── 05.Ingest Results files.py
│   └── 06.Ingest Sprints file.py
│
├── 📁 03-silver/                         # Transformation & cleanse notebooks
│   ├── 01.Transform Circuits Data.py
│   ├── 02.Transform Races Data.py
│   ├── 03.Transform Constructors Data.py
│   ├── 04.Transform Drivers Data.py
│   ├── 05.Transform Results Data.py
│   ├── 05.Transform Results Data (step-by-step).py
│   ├── 05.Transform Results Data (fully-chained).py
│   └── 06.Transform Sprints Data.py
│
├── 📁 04-gold/                           # Aggregation & dimensional model
│   ├── 01.Build Races Dimension.py
│   ├── 02.Build Constructors Dimension.py
│   ├── 03.Build Drivers Dimension.py
│   ├── 04.Build Results Fact.py
│   └── 91.Build Nationality Region Reference.py
│
├── 📁 05-analytics/                      # Analysis & standings views
│   ├── 01.Build Driver Standings View.py
│   └── 02.Build Constructors Standings View.py
│
└── 📁 06-orchestration/                  # Batch control & orchestration
    ├── 00.Create Control Table.py        # Creates batch_control tracking table
    ├── 01.Identify Next Batch.py         # Finds next unprocessed batch folder
    ├── 02.Create New Batch.py            # Marks batch as in_progress
    └── 03.Complete Batch.py              # Marks batch as completed
```

---

## 🗂️ Unity Catalog Setup

### Catalog Structure

```
formula1_incr                          ← Unity Catalog (top-level)
├── landing                            ← Schema for raw volume
│   └── [Volume] files/                ← External Volume over ADLS Gen2
│       ├── 2024-01-01/                ← batch_id folder
│       ├── 2024-01-08/
│       └── ...
│
├── bronze                             ← Schema: raw ingested Delta tables
│   ├── circuits
│   ├── races
│   ├── constructors
│   ├── drivers
│   ├── results
│   └── sprints
│
├── silver                             ← Schema: cleansed & conformed tables
│   ├── circuits
│   ├── races
│   ├── constructors
│   ├── drivers
│   ├── results
│   └── sprints
│
├── gold                               ← Schema: dimensional model
│   ├── dim_races
│   ├── dim_constructors
│   ├── dim_drivers
│   ├── fact_results
│   └── ref_nationality_region
│
└── control                            ← Schema: orchestration metadata
    └── batch_control
```

### Volume Path

Raw source files are accessed via Unity Catalog Volume:
```
/Volumes/formula1_incr/landing/files/<batch_id>/
```

---

## 🥉🥈🥇 Medallion Architecture

### Bronze Layer — Raw Ingestion

Reads raw CSV/JSON files from the ADLS Gen2 landing volume and writes them as **Delta tables partitioned by `batch_id`**, with no business transformations applied.

**Key pattern (`write_to_bronze`):**
```python
def write_to_bronze(input_df, target_table, batch_id):
    final_df = input_df.withColumn('batch_id', f.lit(batch_id))
    (
        final_df.write
        .format('delta')
        .mode('overwrite')
        .partitionBy('batch_id')
        .option('replaceWhere', f"batch_id='{batch_id}'")
        .saveAsTable(target_table)
    )
```

Metadata added via `add_ingestion_metadata()`:
- `current_timestamp` — ingestion time
- `source_file` — source file path from `_metadata.file_path`
- `batch_id` — batch partition identifier

---

### Silver Layer — Cleanse & Upsert

Reads from Bronze Delta tables, applies data type casting, column renaming, null handling, and name standardization. Uses a **Delta MERGE (Upsert)** pattern for idempotent incremental loads.

**Key pattern (`write_to_silver`):**
```python
def write_to_silver(input_df, target_table, merge_condition, columns_to_update):
    final_df = (
        input_df
        .withColumn('created_timestamp', F.current_timestamp())
        .withColumn('updated_timestamp', F.current_timestamp())
    )
    if not spark.catalog.tableExists(target_table):
        final_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    else:
        delta_table = DeltaTable.forName(spark, target_table)
        update_map = {col: f"s.{col}" for col in columns_to_update}
        update_map["updated_timestamp"] = "s.updated_timestamp"
        (
            delta_table.alias("t")
            .merge(final_df.alias("s"), merge_condition)
            .whenMatchedUpdate(condition="s.batch_id >= t.batch_id", set=update_map)
            .whenNotMatchedInsertAll()
            .execute()
        )
```

---

### Gold Layer — Dimensional Model

Reads from Silver tables and builds a **star schema** with dimension and fact tables for analytical consumption:

| Table | Type | Description |
|---|---|---|
| `dim_races` | Dimension | Race calendar enriched with circuit & season info |
| `dim_constructors` | Dimension | Constructor/team master with region mapping |
| `dim_drivers` | Dimension | Driver profiles with nationality region |
| `fact_results` | Fact | Race results — points, position, status per driver per race |
| `ref_nationality_region` | Reference | Nationality to region mapping lookup |

---

## ⚙️ Incremental Load & Batch Orchestration

This project implements a **Control Table pattern** to track incremental batch processing. Each batch corresponds to a dated folder in the ADLS landing volume.

### Batch States

```
Landing folder detected
        │
        ▼
  [in_progress]  ← 02.Create New Batch.py
        │
        ▼
  Pipeline runs (Bronze → Silver → Gold)
        │
        ▼
  [completed]    ← 03.Complete Batch.py
```

### Control Table Schema

```sql
CREATE TABLE formula1_incr.control.batch_control (
    batch_id          string,
    status            string,       -- 'in_progress' | 'completed'
    created_timestamp timestamp,
    updated_timestamp timestamp
)
```

### Batch Identification Logic

`01.Identify Next Batch.py` compares landing folders against the control table and identifies the **earliest unprocessed batch**:

```python
landing_batches = sorted([f.name for f in dbutils.fs.ls(landing_folder_path) if f.isDir()])
tracked_batches = [row.batch_id for row in spark.table(control_table)
                   .filter(col("status").isin("in_progress", "completed"))
                   .collect()]

next_batch = sorted(set(landing_batches) - set(tracked_batches))[0]

# Passes batch_id to downstream tasks via task values
dbutils.jobs.taskValues.set(key="p_batch_id", value=next_batch)
dbutils.jobs.taskValues.set(key="has_batch",  value="true")
```

---

## 🔧 Helper Modules

All helper notebooks are loaded using `%run` at the start of each pipeline notebook:

| Helper | Purpose |
|---|---|
| `01.environment-config` | Defines `catalog_name`, schema names, and `landing_folder_path` |
| `02.bronze-helpers` | `add_ingestion_metadata()`, `write_to_bronze()` |
| `03.silver_helpers` | `write_to_silver()` — full MERGE upsert logic |
| `04.gold_helpers` | Utility functions for Gold layer transformations |

**Usage pattern in every notebook:**
```python
%run ../00-Common/01.environment-config
%run ../00-Common/02.bronze-helpers
```

---

## 📓 Notebooks

### Bronze — Ingestion
| # | Notebook | Source Format | Key Logic |
|---|---|---|---|
| 01 | Ingest Circuits | CSV | Schema inference, metadata |
| 02 | Ingest Races | CSV/JSON | Multi-file read |
| 03 | Ingest Constructors | JSON | Nested structure handling |
| 04 | Ingest Drivers | JSON | Name field parsing |
| 05 | Ingest Results | CSV (multi-file) | Large volume, partitioned write |
| 06 | Ingest Sprints | JSON | Sprint race format |

### Silver — Transformation
| # | Notebook | Key Transformations |
|---|---|---|
| 01 | Transform Circuits | Rename columns, cast types, drop nulls |
| 02 | Transform Races | Date casting, join with circuit |
| 03 | Transform Constructors | Standardize nationality |
| 04 | Transform Drivers | Split full name, cast DOB |
| 05 | Transform Results | Points casting, status mapping, MERGE upsert |
| 06 | Transform Sprints | Sprint-specific result handling |

### Gold — Aggregation
| # | Notebook | Output |
|---|---|---|
| 01 | Build Races Dimension | `dim_races` |
| 02 | Build Constructors Dimension | `dim_constructors` |
| 03 | Build Drivers Dimension | `dim_drivers` |
| 04 | Build Results Fact | `fact_results` |
| 91 | Build Nationality Region Reference | `ref_nationality_region` |

### Analytics
| # | Notebook | Output |
|---|---|---|
| 01 | Build Driver Standings View | Season-wise driver points, wins, rank |
| 02 | Build Constructors Standings View | Season-wise constructor points, rank |

---

## 📊 Dashboards

Interactive dashboards built on **Databricks Dashboards** consuming the Gold layer analytics views:

**Driver Standings Dashboard:**
- Top 10 drivers by season points
- Win count per driver
- Driver rank progression across seasons
- Points breakdown by nationality and region

**Constructor Standings Dashboard:**
- Top constructors by season
- Year-over-year team performance trends
- Points distribution across constructors
- Regional breakdown via nationality reference

---

## ▶️ How to Run

### 1. Clone the Repository

In Databricks:
```
Workspace → Repos → Add Repo → paste your GitHub URL
```

### 2. Run One-Time Setup

```
01-setup/01.Setup Project Environment.sql   ← creates catalogs, schemas, volumes
01-setup/02.Setup Batch Events.py           ← seeds batch reference data
06-orchestration/00.Create Control Table.py ← creates batch_control table
```

### 3. Place Raw Data in Landing Volume

Upload batch folders to:
```
/Volumes/formula1_incr/landing/files/<batch_id>/
```
Example:
```
/Volumes/formula1_incr/landing/files/2024-01-01/circuits.csv
/Volumes/formula1_incr/landing/files/2024-01-01/races.json
...
```

### 4. Run Orchestration to Identify Batch

```
06-orchestration/01.Identify Next Batch.py   ← sets p_batch_id task value
06-orchestration/02.Create New Batch.py      ← marks batch as in_progress
```

### 5. Run Pipeline in Order

```
Step 1 — Bronze:    02-bronze/01 through 06     (ingest raw files)
Step 2 — Silver:    03-silver/01 through 06     (cleanse & upsert)
Step 3 — Gold:      04-gold/01 through 04       (build dimensional model)
Step 4 — Analytics: 05-analytics/01 and 02      (build standings views)
```

### 6. Complete the Batch

```
06-orchestration/03.Complete Batch.py           ← marks batch as completed
```

### 7. View Dashboards

Navigate to **Databricks Dashboards** in the workspace to view Driver and Constructor Standings visualizations.

---

## 🔑 Key Technical Highlights

- **Incremental Batch Load** using a Control Table pattern with `batch_id` tracking — prevents reprocessing and supports reruns
- **Delta Lake MERGE (Upsert)** in Silver layer — idempotent, handles both new records and updates
- **`replaceWhere` partition overwrite** in Bronze — safe re-ingestion of a specific batch without affecting other partitions
- **Unity Catalog Volumes** for governed, credential-free access to ADLS Gen2 raw files
- **Star Schema** in Gold — `dim_races`, `dim_drivers`, `dim_constructors`, `fact_results` for optimized analytical queries
- **Helper module pattern** using `%run` — promotes code reuse and separation of concerns across all layers
- **Task Values (`dbutils.jobs.taskValues`)** — passes `batch_id` between Databricks Workflow tasks dynamically

---

## 👤 Author

**Javed**
Lead Technical Consultant | Azure Data Engineer
[LinkedIn](https://linkedin.com/in/your-profile) | [GitHub](https://github.com/your-username)

---

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

*Built with ❤️ using Azure Databricks, Delta Lake, Unity Catalog & PySpark*
ENDOFREADME

echo "README created successfully"
wc -l /tmp/README.md
