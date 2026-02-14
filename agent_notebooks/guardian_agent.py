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

dbutils.widgets.text("table_name","")


# COMMAND ----------

TABLE_NAME = dbutils.widgets.get("table_name")

# COMMAND ----------

TABLE_NAME

# COMMAND ----------

# TABLE_NAME = "bronze_orders"
CATALOG = "agentic_ai_catalog"
SCHEMA = "guardian_schema"

BRONZE_TABLE = f"{CATALOG}.sales_dlt_table.{TABLE_NAME}"
SCHEMA_HISTORY_TABLE = f"{CATALOG}.{SCHEMA}.schema_history"
METRICS_TABLE = f"{CATALOG}.{SCHEMA}.bronze_quality_metrics"
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.guardian_data_quality_audit"
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.guardian_agent_control"

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

spark.createDataFrame(
    [(TABLE_NAME, df.schema.json())],
    ["table_name","schema_json"]
).withColumn(
    "captured_at", current_timestamp()
).write.mode("append").saveAsTable(SCHEMA_HISTORY_TABLE)

# COMMAND ----------

metrics_rows = []

for c in df.columns:
    row = df.select(
        (count(when(col(c).isNull(),1))/count("*")*100).alias("null_pct"),
        countDistinct(col(c)).alias("distinct_cnt")
    ).first()

    metrics_rows.append((TABLE_NAME,c,float(row[0]),int(row[1])))

spark.createDataFrame(
    metrics_rows,
    ["table_name","column_name","null_percent","distinct_count"]
).withColumn(
    "captured_at", current_timestamp()
).write.mode("append").saveAsTable(METRICS_TABLE)


# COMMAND ----------

# ------------------------------------------
# STEP 3: FETCH LAST TWO SCHEMAS
# ------------------------------------------

schemas = spark.sql(f"""
SELECT schema_json
FROM {SCHEMA_HISTORY_TABLE}
WHERE table_name='{TABLE_NAME}'
ORDER BY captured_at DESC
LIMIT 2
""").collect()

if len(schemas) == 0:
    previous_schema = "{}"
    current_schema = "{}"
else:
    previous_schema = schemas[1][0] if len(schemas) > 1 else "{}"
    current_schema = schemas[0][0]

metrics_df = spark.sql(f"""
SELECT *
FROM {METRICS_TABLE}
WHERE table_name='{TABLE_NAME}'
ORDER BY captured_at DESC
""").toPandas()


# COMMAND ----------

# ------------------------------------------
# STEP 4: PROMPT LLM AGENT
# ------------------------------------------

prompt = f"""
You are a Data Quality Guardian.

Detect:
- Schema drift
- Null spikes
- Invalid enum values
- Dangerous anomalies

Previous schema:
{previous_schema}

Current schema:
{current_schema}

Latest metrics:
{metrics_df.to_json()}

Ignore the _rescued_data column and don't perform any analysis on it.
Perform your analysis on the rest of the columns and give me all the possible issues and also provide the strategy, silver_filter_codition and the correction expression accordingly in the below json format.

Return JSON strictly:

{{
 "issues":[
   {{
     "issue_type":"",
     "severity":"LOW|MEDIUM|HIGH",
     "column":"",
     "message":""
   }}
 ],
 "overall_severity":"LOW|MEDIUM|HIGH",
 "recommended_fix_code": {{
    "strategy":"QUARANTINE|CORRECT|BOTH|BLOCK",
    "silver_filter_condition":"",
    "correction_expression": {{}}
 }}
}}
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
if json_str:
    agent_result = json.loads(json_str)
    print("Agent Decision:")
    print(agent_result)
else:
    print("LLM response is empty or not valid JSON.")


# COMMAND ----------

rows = []

for issue in agent_result["issues"]:
    rows.append((
        TABLE_NAME,
        issue["issue_type"],
        issue["severity"],
        issue["column"],
        issue["message"],
        json.dumps(agent_result["recommended_fix_code"]),
        agent_result["overall_severity"]
    ))

spark.createDataFrame(
    rows,
    ["table_name","issue_type","severity",
     "column_name","message",
     "recommended_fix_code","overall_severity"]
).withColumn(
    "event_time", current_timestamp()
).write.mode("append").saveAsTable(AUDIT_TABLE)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# -----------------------------
# Build single control record
# -----------------------------

allow = False if agent_result["overall_severity"] == "HIGH" else True

control_df = spark.createDataFrame(
    [(
        TABLE_NAME,
        allow,
        agent_result["overall_severity"],
        json.dumps(agent_result["recommended_fix_code"])
    )],
    ["table_name","allow_processing","severity","fix_json"]
).withColumn(
    "updated_at", current_timestamp()
)

control_df.createOrReplaceTempView("control_updates")

# -----------------------------
# MERGE INTO CONTROL TABLE
# -----------------------------

spark.sql(f"""
MERGE INTO {CONTROL_TABLE} t
USING control_updates s
ON t.table_name = s.table_name

WHEN MATCHED THEN UPDATE SET
  t.allow_processing = s.allow_processing,
  t.severity = s.severity,
  t.fix_json = s.fix_json,
  t.updated_at = s.updated_at

WHEN NOT MATCHED THEN INSERT *
""")

print("Control table upserted successfully")


# COMMAND ----------

rows = []
PIPELINE_NAME = "AI_BASED_SALES_PIPELINE"

for issue in agent_result["issues"]:
    rows.append((
        PIPELINE_NAME,
        TABLE_NAME,
        issue["issue_type"],
        issue["severity"],
        issue["message"],
        json.dumps(agent_result["recommended_fix_code"]),
        allow,
        True
    ))

spark.createDataFrame(
    rows,
    ["pipeline_name","table_name","issue_type","severity","message",
     "action_taken","processing_allowed","fix_applied"]
).withColumn(
    "event_time", current_timestamp()
).write.mode("append").saveAsTable("agentic_ai_catalog.guardian_schema.guardian_agent_events")


# COMMAND ----------

print(f"Guardian Agent Finished For Table : {TABLE_NAME}")