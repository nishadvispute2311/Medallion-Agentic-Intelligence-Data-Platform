# Databricks notebook source
# MAGIC %pip install -U -qqqq mlflow databricks-openai databricks-agents

# COMMAND ----------

# MAGIC %pip show databricks-agents

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

import json
from pyspark.sql.functions import *
from databricks.sdk import WorkspaceClient
import mlflow
from pyspark.sql import SparkSession
import json

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

CATALOG = "agentic_ai_catalog"
SCHEMA = "agent_test"

# COMMAND ----------

# ============================================================
# 1. Load Laws
# ============================================================
laws = spark.sql(f"""
  SELECT policy_json
  FROM {CATALOG}.{SCHEMA}.policy_control_laws
  WHERE is_active = true
  ORDER BY created_at DESC
  LIMIT 1
""").collect()[0]["policy_json"]

# COMMAND ----------

# ============================================================
# 2. Compute Metrics from Silver
# ============================================================
metrics = {}

# ---- Customers
metrics["customers"] = spark.sql(f"""
  SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN age < 0 OR age > 120 THEN 1 ELSE 0 END) AS invalid_age_count
  FROM {CATALOG}.{SCHEMA}.silver_customers
""").toPandas().to_dict("records")[0]

# ---- Orders
metrics["orders"] = spark.sql(f"""
  SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) AS invalid_amount,
    SUM(CASE WHEN discount_amount > amount THEN 1 ELSE 0 END) AS invalid_discount
  FROM {CATALOG}.{SCHEMA}.silver_orders
""").toPandas().to_dict("records")[0]

# ---- Payments
metrics["payments"] = spark.sql(f"""
  SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN payment_status IN ('FAILED','PENDING') THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS failure_rate
  FROM {CATALOG}.{SCHEMA}.silver_payments
""").toPandas().to_dict("records")[0]

# ---- Refunds
metrics["refunds"] = spark.sql(f"""
  SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN refund_amount <= 0 THEN 1 ELSE 0 END) AS invalid_refunds
  FROM {CATALOG}.{SCHEMA}.silver_refunds
""").toPandas().to_dict("records")[0]

# COMMAND ----------

# DBTITLE 1,3. LLM Prompt
# ============================================================
# 3. LLM Prompt
# ============================================================
import json
import re
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

prompt = f"""
You are a Data Governance Policy Agent.

Below are:
1. Current Silver layer metrics
2. Policy laws

Your task:
- Analyze metrics against laws
- Decide governance actions
- Return ONLY a valid JSON
- Follow the JSON schema exactly
- Most Important : analyze metrics against the laws and return the json in such a way that per table what all laws are violated and what are the actions to take

Metrics:
{json.dumps(metrics, indent=2, default=decimal_default)}

Laws:
{laws}


JSON FORMAT (STRICT):
{{
  "strategy": "",
  "row_rules": [
    {{
      "table": "",
      "rule_name": "",
      "expression": "",
      "severity": "",
      "on_fail": ""
    }}
  ],
  "metric_rules": [
    {{
      "table": "",
      "metric": "",
      "threshold_pct": 0,
      "severity": "",
      "on_fail": ""
    }}
  ]
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

# DBTITLE 1,Cell 12
import json
pipeline_name = "silver_to_gold_pipeline"

table_name_list = [
  "customers",
  "orders",
  "payments",
  "refunds"
]

from pyspark.sql.functions import current_timestamp

# Serialize agent_result to JSON string
policy_decision_json = json.dumps(agent_result)

data = [
    (
        pipeline_name,
        table_name_list,
        policy_decision_json
    )
]

df = spark.createDataFrame(
    data,
    ["pipeline_name", "table_name_list", "policy_decision_json"]
).withColumn("updated_timestamp", current_timestamp())

df.createOrReplaceTempView("policy_agent_output")

spark.sql("""
MERGE INTO agentic_ai_catalog.agent_test.policy_control tgt
USING policy_agent_output src
ON tgt.pipeline_name = src.pipeline_name
WHEN MATCHED THEN
  UPDATE SET
    tgt.table_name_list = src.table_name_list,
    tgt.policy_decision_json = src.policy_decision_json,
    tgt.updated_timestamp = src.updated_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    pipeline_name,
    table_name_list,
    policy_decision_json,
    updated_timestamp
  )
  VALUES (
    src.pipeline_name,
    src.table_name_list,
    src.policy_decision_json,
    src.updated_timestamp
  )
""")