# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Lakebase Setup — Streamline Telco
# MAGIC %md
# MAGIC # Milestone 2: Lakebase Postgres Setup
# MAGIC
# MAGIC **Project:** Streamline Telco — Subscriber Retention  
# MAGIC **Source:** `demo_tech_telco.streaming.gold_subscriber_position` (materialized view, 40K subscribers)  
# MAGIC **Target:** Lakebase Postgres `streamline-telco` project → `streaming.synced_subscriber_position`  
# MAGIC
# MAGIC This notebook provisions a Lakebase Postgres instance, registers it in Unity Catalog, and sets up a synced table so the operational app has sub-millisecond access to the at-risk subscriber view.

# COMMAND ----------

# DBTITLE 1,Step 1: Upgrade SDK
import importlib.metadata as md
import subprocess, sys

try:
    before = md.version("databricks-sdk")
except md.PackageNotFoundError:
    before = None

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "databricks-sdk>=0.118.0"])

after = md.version("databricks-sdk")
print(f"databricks-sdk: {before} -> {after}  (changed={before != after})")

if before != after:
    print("Version changed — restarting Python to load the new SDK...")
    dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Step 2: Create Lakebase Project
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Project, ProjectSpec

w = WorkspaceClient()

# Create the Lakebase Postgres project (auto-provisions production branch + primary endpoint)
try:
    op = w.postgres.create_project(
        project=Project(spec=ProjectSpec(display_name="Streamline Telco", pg_version=17)),
        project_id="streamline-telco",
    )
    project = op.wait()
    print(f"✓ Created project: {project.name}")
    print(f"  Display name: {project.spec.display_name}")
    print(f"  PG version: {project.spec.pg_version}")
except Exception as e:
    if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
        print("Project 'streamline-telco' already exists — fetching existing project info instead.")
        project = w.postgres.get_project(name="projects/streamline-telco")
        print(f"✓ Existing project: {project.name}")
        spec = getattr(project, "spec", None)
        if spec is not None:
            print(f"  Display name: {getattr(spec, 'display_name', 'N/A')}")
            print(f"  PG version: {getattr(spec, 'pg_version', 'N/A')}")
        else:
            print(f"  (spec not populated on get_project response)")
            print(f"  Full object: {project}")
    else:
        raise

# COMMAND ----------

# DBTITLE 1,Step 3: Verify Project Resources
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

print("=== Branches ===")
for b in w.postgres.list_branches(parent="projects/streamline-telco"):
    print(f"  {b.name}  state={b.status.current_state}")

print("\n=== Endpoints ===")
for e in w.postgres.list_endpoints(parent="projects/streamline-telco/branches/production"):
    print(f"  {e.name}")
    print(f"  Host: {e.status.hosts.host}")
    print(f"  State: {e.status.current_state}")

print("\n=== Databases ===")
for d in w.postgres.list_databases(parent="projects/streamline-telco/branches/production"):
    print(f"  {d.name}")

# COMMAND ----------

# DBTITLE 1,Step 4: Create Synced Table (Delta → Lakebase)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    SyncedTable,
    SyncedTableSyncedTableSpec,
    SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy,
)

w = WorkspaceClient()

# Sync gold_subscriber_position (materialized view) into Lakebase
# Using SNAPSHOT mode since MVs don't support Change Data Feed
try:
    op = w.postgres.create_synced_table(
        synced_table=SyncedTable(spec=SyncedTableSyncedTableSpec(
            source_table_full_name="demo_tech_telco.streaming.gold_subscriber_position",
            branch="projects/streamline-telco/branches/production",
            primary_key_columns=["subscriber_id"],
            scheduling_policy=SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy.SNAPSHOT,
            postgres_database="databricks-postgres",
            create_database_objects_if_missing=True,
        )),
        synced_table_id="demo_tech_telco.streaming.synced_subscriber_position",
    )
    result = op.wait()
    print(f"✓ Synced table created: {result.name}")
    print(f"  Pipeline: {result.status.pipeline_id}")
    print(f"  State: {result.status.detailed_state}")
    print(f"  UC state: {result.status.unity_catalog_provisioning_state}")
except Exception as e:
    if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
        print("Synced table 'demo_tech_telco.streaming.synced_subscriber_position' already exists — fetching existing status instead.")
        result = w.postgres.get_synced_table(name="synced_tables/demo_tech_telco.streaming.synced_subscriber_position")
        print(f"✓ Existing synced table: {result.name}")
        print(f"  Pipeline: {result.status.pipeline_id}")
        print(f"  State: {result.status.detailed_state}")
        print(f"  UC state: {result.status.unity_catalog_provisioning_state}")
    else:
        raise

# COMMAND ----------

# DBTITLE 1,Step 5: Poll Sync Status
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for i in range(20):
    st = w.postgres.get_synced_table(name="synced_tables/demo_tech_telco.streaming.synced_subscriber_position")
    state = st.status.detailed_state if st.status else "UNKNOWN"
    progress = st.status.ongoing_sync_progress if st.status else None
    synced_rows = progress.synced_row_count if progress else None
    total_rows = progress.total_row_count if progress else None
    
    print(f"[{i}] State: {state} | Synced: {synced_rows}/{total_rows}")
    
    if "ONLINE" in str(state):
        print("\n✓ Sync complete!")
        break
    time.sleep(15)
else:
    print("\nStill provisioning — check status manually later")

print(f"\nPipeline ID: {st.status.pipeline_id}")

# COMMAND ----------

# DBTITLE 1,Step 6: Verify — Query Lakebase Postgres Directly
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q"])

from databricks.sdk import WorkspaceClient
import psycopg2

w = WorkspaceClient()

# Generate OAuth credential
cred = w.postgres.generate_database_credential(
    endpoint="projects/streamline-telco/branches/production/endpoints/primary"
)

# Connect: user = Databricks email, password = OAuth token
conn = psycopg2.connect(
    host="ep-winter-mountain-d2nith27.database.us-east-1.cloud.databricks.com",
    port=5432,
    dbname="databricks-postgres",
    user="roshni.agarwal@databricks.com",
    password=cred.token,
    sslmode="require"
)
print("✓ Connected to Lakebase Postgres!")
cur = conn.cursor()

# Row count
cur.execute("SELECT COUNT(*) FROM streaming.synced_subscriber_position")
count = cur.fetchone()[0]
print(f"\n=== Row count: {count:,} ===")

# Hero subscriber
cur.execute("""
    SELECT subscriber_id, plan_type, risk_band, churn_risk_score, clv_at_risk_usd 
    FROM streaming.synced_subscriber_position 
    WHERE subscriber_id = 'SUB-0000214'
""")
print("\n=== Hero subscriber (SUB-0000214) ===")
for row in cur.fetchall():
    print(f"  {row[0]} | plan={row[1]} | band={row[2]} | risk={row[3]:.2f} | CLV_at_risk=${row[4]:,.0f}")

# Top 5 critical
cur.execute("""
    SELECT subscriber_id, plan_type, risk_band, churn_risk_score, clv_at_risk_usd 
    FROM streaming.synced_subscriber_position 
    WHERE risk_band = 'critical'
    ORDER BY clv_at_risk_usd DESC
    LIMIT 5
""")
print("\n=== Top 5 critical subscribers by CLV at risk ===")
for row in cur.fetchall():
    print(f"  {row[0]} | plan={row[1]} | risk={row[3]:.2f} | CLV_at_risk=${row[4]:,.0f}")

cur.close()
conn.close()
print("\n✓ Lakebase synced table verified — serving live at-risk subscriber data!")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Results
# MAGIC
# MAGIC | Resource | Value |
# MAGIC |----------|-------|
# MAGIC | **Lakebase Project** | `streamline-telco` (Postgres 17) |
# MAGIC | **Branch** | `production` (READY) |
# MAGIC | **Endpoint** | `primary` — `ep-winter-mountain-d2nith27.database.us-east-1.cloud.databricks.com` |
# MAGIC | **Database** | `databricks-postgres` |
# MAGIC | **Synced Table (UC)** | `demo_tech_telco.streaming.synced_subscriber_position` |
# MAGIC | **Synced Table (PG)** | `streaming.synced_subscriber_position` |
# MAGIC | **Sync Mode** | SNAPSHOT (MV source — CDF not available) |
# MAGIC | **Rows synced** | 40,000 |
# MAGIC | **Pipeline** | `b5b15989-c646-4b2a-b024-48957a9789e5` |
# MAGIC
# MAGIC The operational app can now query `streaming.synced_subscriber_position` in Lakebase for sub-millisecond per-subscriber lookups — the same data that powers the AI/BI dashboard, governed by the same Unity Catalog policies.