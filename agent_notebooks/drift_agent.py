# Databricks notebook source
# %pip install -U -qqqq mlflow databricks-openai databricks-agents

# COMMAND ----------

# %pip show databricks-agents

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

import json
from pyspark.sql.functions import *
from databricks.sdk import WorkspaceClient
import mlflow

# COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("table_type", "")   # TRANSACTIONAL | DIMENSION



# COMMAND ----------

TABLE_NAME = dbutils.widgets.get("table_name")
TABLE_TYPE = dbutils.widgets.get("table_type")

# COMMAND ----------

print(f"{TABLE_NAME} | {TABLE_TYPE}")

# COMMAND ----------

CATALOG = "agentic_ai_catalog"
SCHEMA = "drift_schema"

BRONZE_TABLE = f"{CATALOG}.sales_dlt_table.{TABLE_NAME}"

METRICS_TABLE = f"{CATALOG}.{SCHEMA}.drift_metrics_history"
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.drift_audit_table"
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.drift_control_table"

# COMMAND ----------

LLM_ENDPOINT_NAME = None

from databricks.sdk import WorkspaceClient
def is_endpoint_available(endpoint_name):
  try:
    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    client.chat.completions.create(model=endpoint_name, messages=[{"role": "user", "content": "What is AI?"}])
    return True
  except Exception:
    return False
  
client = WorkspaceClient()
for candidate_endpoint_name in ["databricks-claude-3-7-sonnet", "databricks-meta-llama-3-3-70b-instruct"]:
    if is_endpoint_available(candidate_endpoint_name):
      LLM_ENDPOINT_NAME = candidate_endpoint_name
assert LLM_ENDPOINT_NAME is not None, "Please specify LLM_ENDPOINT_NAME"


# COMMAND ----------

# ------------------------------------------
# ENABLE LOGGING
# ------------------------------------------

from databricks.sdk import WorkspaceClient
import mlflow

mlflow.openai.autolog()
openai_client = WorkspaceClient().serving_endpoints.get_open_ai_client()

# COMMAND ----------

df = spark.table(BRONZE_TABLE)

# COMMAND ----------

# ============================================================
# STEP 1 — COLLECT METRICS
# ============================================================

metrics = []

# ---------------------------
# Row count
# ---------------------------
metrics.append(("row_count", df.count()))

# ---------------------------
# Column metrics
# ---------------------------
for c in df.columns:
    r = df.select(
        (count(when(col(c).isNull(),1))/count("*")*100).alias("null_pct"),
        countDistinct(col(c)).alias("distinct_cnt")
    ).first()

    metrics.append((f"{c}_null_pct", float(r[0])))
    metrics.append((f"{c}_distinct_cnt", int(r[1])))


# COMMAND ----------

# ---------------------------
# Numeric distribution
# ---------------------------
numeric_cols = [f.name for f in df.schema.fields 
                if f.dataType.simpleString() in ["int","bigint","double","float"]]

for c in numeric_cols:
    avg_v = df.select(avg(c)).first()[0]
    metrics.append((f"{c}_avg", float(avg_v) if avg_v else 0))

# ---------------------------
# Categorical distribution
# ---------------------------
string_cols = [f.name for f in df.schema.fields 
               if f.dataType.simpleString()=="string"
               and f.name != "_rescued_data"]

total = df.count()

for c in string_cols:
    rows = (
        df.groupBy(c)
          .count()
          .withColumn("pct", col("count")/lit(total)*100)
          .limit(50)
          .collect()
    )

    for r in rows:
        metrics.append((f"{c}::{str(r[0])}", float(r["pct"])))


# COMMAND ----------

print(metrics)

# COMMAND ----------

# ============================================================
# STEP 2 — SAVE METRICS
# ============================================================

spark.createDataFrame(
    [(TABLE_NAME, m[0], float(m[1])) for m in metrics],
    ["table_name","metric_name","metric_value"]
).withColumn(
    "captured_at", current_timestamp()
).write.mode("append").saveAsTable(METRICS_TABLE)

# COMMAND ----------

# ============================================================
# STEP 3 — FETCH LAST 2 SNAPSHOTS
# ============================================================

hist = spark.sql(f"""
SELECT metric_name, metric_value, captured_at
FROM {METRICS_TABLE}
WHERE table_name='{TABLE_NAME}'
ORDER BY captured_at DESC
""").toPandas()

if hist.empty:
    print("First run - skipping drift detection")
    dbutils.notebook.exit("OK")

latest_ts = hist.iloc[0]["captured_at"]

current_metrics = hist[hist.captured_at==latest_ts] \
                    .set_index("metric_name")["metric_value"].to_dict()

previous_metrics = (
    hist[hist.captured_at < latest_ts]
    .groupby("metric_name")
    .first()["metric_value"]
    .to_dict()
)

# COMMAND ----------

# DBTITLE 1,STEP 4 — PROMPT LLM
# ============================================================
# STEP 4 — PROMPT LLM
# ============================================================

prompt = f"""
You are a Drift Detection and Decision Agent.

Table type: {TABLE_TYPE}

Previous metrics:
{previous_metrics}

Current metrics:
{current_metrics}

NOTE : FOR 1ST TIME RUN PLEASE IGNORE THE VOLUME DRIFT ISSUE

Detect the following drift types and ignore the columns order_id, payments_id, customer_id, refund_id from the NUMERIC_DRIFT and perform distinct count operation only on the columns order_id, payments_id, customer_id, refund_id:

1. VOLUME_DRIFT        -> large change in row_count
2. NULL_DRIFT          -> sudden change in null percentages
3. CARDINALITY_DRIFT   -> large change in distinct counts
4. NUMERIC_DRIFT       -> large change in numeric averages
5. CATEGORY_DRIFT      -> large distribution shifts in categorical values

------------------------------------------------------------
GLOBAL BUSINESS RULES (APPLY TO ALL TABLES)
------------------------------------------------------------

A) VOLUME DRIFT (row_count)
- Calculate percent change:
  abs(current - previous) / previous * 100

Rules:
- > 50% change → HIGH severity
- 25% - 50% change → MEDIUM severity
- < 25% → LOW severity

------------------------------------------------------------

B) NULL DRIFT

Rules:
- If null percentage INCREASES → BAD
- If null percentage DECREASES → GOOD

Severity:
- Increase > 40% → HIGH
- Increase 20% - 40% → MEDIUM
- Else → LOW

------------------------------------------------------------

C) CARDINALITY DRIFT (primary keys)

Primary keys:
- orders → order_id
- payments → payment_id
- refunds → refund_id
- customers → customer_id

Rules:
- Distinct decrease > 30% → HIGH
- Distinct decrease 15% - 30% → MEDIUM

If distinct INCREASES:
- Check related success-like category distribution:
  - If success-dominated → GOOD
  - If failure/negative dominated → BAD

If increase + BAD → MEDIUM or HIGH

------------------------------------------------------------

D) NUMERIC DRIFT (averages)

Numeric columns:
- orders → amount, discount_amount
- payments → amount
- refunds → refund_amount
- customers → age

Rules:
- Average decrease > 70% → HIGH
- Average decrease 40% - 70% → MEDIUM
- Average increase > 90% → MEDIUM
- Else → LOW

------------------------------------------------------------

E) CATEGORY DRIFT (status columns)

Category columns:
- orders → order_status, payment_status
- payments → payment_status
- refunds → refund_status
- customers → city

Positive categories:
SUCCESS, COMPLETED, APPROVED, ACTIVE

Negative categories:
FAILED, PENDING, UNKNOWN, CANCELLED, REJECTED

Rules:
- Positive category decreases > 30% → HIGH
- Negative category increases > 50% → HIGH
- Moderate shift → MEDIUM
- Minor shift → LOW

------------------------------------------------------------

IGNORED COLUMNS FOR DRIFT:
product_id, order_id, customer_id, refund_id

------------------------------------------------------------

RECOMMENDED ACTION MAPPING:

- HIGH severity → BLOCK_SILVER
- MEDIUM severity → FLAG_AND_ALERT
- LOW severity → INVESTIGATE_SOURCE or ALLOW


For each detected drift:

- Assign severity
- Provide human readable message
- Recommend action

Allowed recommended_action values:

INVESTIGATE_SOURCE  
BLOCK_SILVER  
FLAG_AND_ALERT  
ALLOW  

Return JSON ONLY in this exact format:

{{
 "drifts":[
   {{
     "issue_type":"",
     "column":"",
     "severity":"LOW|MEDIUM|HIGH",
     "message":"",
     "recommended_action":""
   }}
 ],
 "overall_severity":"LOW|MEDIUM|HIGH"
}}

Rules:
- If severity HIGH → recommended_action should usually be BLOCK_SILVER
- If severity MEDIUM → FLAG_AND_ALERT
- If severity LOW → INVESTIGATE_SOURCE or ALLOW
"""



response = openai_client.chat.completions.create(
    model=LLM_ENDPOINT_NAME,
    messages=[{"role":"user","content":prompt}]
)

# Print the raw LLM response for diagnosis
print("Raw LLM response:")
print(response.choices[0].message.content)

raw_content = response.choices[0].message.content.strip()

# Extract JSON block if present
import re
json_match = re.search(r'```json(.*?)```', raw_content, re.DOTALL)
if not json_match:
    json_match = re.search(r'```(.*?)```', raw_content, re.DOTALL)
if json_match:
    json_str = json_match.group(1).strip()
else:
    json_str = raw_content

# Attempt to parse JSON only if response is not empty
if json_str and json_str.strip() and json_str != '' and json_str != '\n':
    agent_result = json.loads(json_str)
    print("Agent Decision:")
    print(agent_result)
else:
    print("LLM response is empty or not valid JSON.")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

rows = []

for d in agent_result["drifts"]:
    rows.append((
        TABLE_NAME,
        d.get("issue_type"),
        d.get("column"),
        d.get("severity"),
        d.get("message"),
        d.get("recommended_action"),
        agent_result["overall_severity"]
    ))

# ============================================================
# STEP 5 — WRITE TO AUDIT TABLE
# HANDLE EMPTY RESULT
if len(rows) == 0:
    spark.createDataFrame(
        [(TABLE_NAME, "NO_DRIFT", "NONE", "LOW",
          "No drift detected", "NONE", "LOW")],
        ["table_name","issue_type","column","severity",
         "message","recommended_action","overall_severity"]
    ).withColumn("event_time", current_timestamp()) \
     .write.mode("append").saveAsTable(AUDIT_TABLE)
else:
    spark.createDataFrame(
        rows,
        ["table_name",
         "issue_type",
         "column",
         "severity",
         "message",
         "recommended_action",
         "overall_severity"]
    ).withColumn(
        "event_time", current_timestamp()
    ).write.mode("append").saveAsTable(AUDIT_TABLE)

    print(f"{len(rows)} drift records written to audit table")


# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import current_timestamp
import json

# ============================================================
# STEP 6 — UPDATE CONTROL TABLE (MERGE UPSERT) 2
# ============================================================

actions = []

if "drifts" in agent_result:
    actions = [d["recommended_action"] for d in agent_result["drifts"]]
allow = False if "BLOCK_SILVER" in actions else True

updates_df = spark.createDataFrame(
    [(TABLE_NAME, allow, agent_result["overall_severity"],json.dumps(agent_result))],
    ["table_name", "allow_processing", "severity","agent_result_json"]
).withColumn(
    "updated_at", current_timestamp()
)


# MERGE (UPSERT)
updates_df.createOrReplaceTempView("updates")

spark.sql(f"""
MERGE INTO agentic_ai_catalog.drift_schema.drift_control_table AS tgt
USING updates AS src
ON tgt.table_name = src.table_name

WHEN MATCHED THEN UPDATE SET
  tgt.allow_processing = src.allow_processing,
  tgt.severity = src.severity,
  tgt.agent_result_json = src.agent_result_json,
  tgt.updated_at = src.updated_at

WHEN NOT MATCHED THEN INSERT (
  table_name,
  allow_processing,
  severity,
  agent_result_json,
  updated_at
)
VALUES (
  src.table_name,
  src.allow_processing,
  src.severity,
  src.agent_result_json,
  src.updated_at
)
""")


# COMMAND ----------

print(f"Drift Agent Completed Successfully : {BRONZE_TABLE}")