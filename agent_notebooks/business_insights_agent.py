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
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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

LLM_ENDPOINT_NAME

# COMMAND ----------

# ------------------------------------------
# ENABLE LOGGING
# ------------------------------------------

from databricks.sdk import WorkspaceClient
import mlflow

mlflow.openai.autolog()
openai_client = WorkspaceClient().serving_endpoints.get_open_ai_client()

# COMMAND ----------

revenue = (
    spark.table("agentic_ai_catalog.agent_test.kpi_daily_order_revenue")
    .filter(F.col("net_revenue").isNotNull())
)

payment = (
    spark.table("agentic_ai_catalog.agent_test.kpi_payment_success_rate")
    .filter(F.col("payment_success_rate").isNotNull())
)

refund = (
    spark.table("agentic_ai_catalog.agent_test.kpi_refund_rate")
    .filter(F.col("refund_rate").isNotNull())
)

discount = (
    spark.table("agentic_ai_catalog.agent_test.kpi_discount_impact")
    .filter(F.col("gross_revenue").isNotNull())
)


# COMMAND ----------

from pyspark.sql import functions as F

rev = revenue.alias("rev")
pay = payment.alias("pay")
ref = refund.alias("ref")
disc = discount.alias("disc")

daily_metrics = (
    rev
    .join(pay, F.col("rev.order_date") == F.col("pay.order_date"), "left")
    .join(ref, F.col("rev.order_date") == F.col("ref.order_date"), "left")
    .join(disc, F.col("rev.order_date") == F.col("disc.order_date"), "left")
)

daily_metrics = daily_metrics.withColumn(
    "discount_pct",
    F.when(
        F.col("rev.gross_revenue") > 0,
        F.round(
            F.col("disc.total_discount") / F.col("rev.gross_revenue") * 100,
            2
        )
    ).otherwise(F.lit(0.0))
)

final_daily_metrics = daily_metrics.select(
    F.col("rev.order_date"),
    F.col("rev.total_orders"),
    F.col("rev.gross_revenue"),
    F.col("rev.net_revenue"),
    F.col("pay.payment_success_rate"),
    F.col("ref.refund_rate"),
    F.col("disc.total_discount"),
    F.col("discount_pct")
)



# COMMAND ----------

final_daily_metrics.display()

# COMMAND ----------

w = Window.orderBy("order_date")

daily_trends = (
    final_daily_metrics
    .withColumn("net_revenue_prev", F.lag("net_revenue", 1).over(w))
    .withColumn("orders_prev", F.lag("total_orders", 1).over(w))

    .withColumn(
        "net_revenue_wow_pct",
        F.when(
            F.col("net_revenue_prev").isNotNull(),
            F.round(
                (F.col("net_revenue") - F.col("net_revenue_prev")) /
                F.col("net_revenue_prev") * 100, 2
            )
        )
    )
    .withColumn(
        "orders_wow_pct",
        F.when(
            F.col("orders_prev").isNotNull(),
            F.round(
                (F.col("total_orders") - F.col("orders_prev")) /
                F.col("orders_prev") * 100, 2
            )
        )
    )
)


# COMMAND ----------

signals = (
    daily_trends
    .withColumn(
        "payment_health",
        F.when(F.col("payment_success_rate") >= 97, "HEALTHY")
         .when(F.col("payment_success_rate") >= 95, "WARNING")
         .otherwise("CRITICAL")
    )
    .withColumn(
        "refund_risk",
        F.when(F.col("refund_rate") < 3, "LOW")
         .when(F.col("refund_rate") <= 8, "MEDIUM")
         .otherwise("HIGH")
    )
    .withColumn(
        "discount_health",
        F.when(F.col("discount_pct") < 10, "EFFICIENT")
         .when(F.col("discount_pct") <= 20, "MODERATE")
         .otherwise("WASTEFUL")
    )
    .withColumn(
        "risk_flag",
        (F.col("refund_rate") > 8) |
        (F.col("payment_success_rate") < 95) |
        (F.col("discount_pct") > 20)
    )
)


# COMMAND ----------

llm_input = (
    signals
    .select(
        F.col("order_date").alias("date"),
        "net_revenue",
        "net_revenue_wow_pct",
        "total_orders",
        "orders_wow_pct",
        "payment_success_rate",
        "payment_health",
        "refund_rate",
        "refund_risk",
        "discount_pct",
        "discount_health",
        "risk_flag"
    )
)


# COMMAND ----------

# ------------------------------------------
# STEP 4: PROMPT LLM AGENT
# ------------------------------------------

prompt = f"""
You are a senior business analyst.

Given the following daily metrics, do the tasks below.
Do NOT invent numbers. Use only the data provided.

Data:
- Date : {{date}}
- Net revenue change (%): {{net_revenue_wow_pct}}
- Orders change (%): {{orders_wow_pct}}
- Payment success rate: {{payment_success_rate}} ({{payment_health}})
- Refund rate: {{refund_rate}} ({{refund_risk}})
- Discount usage: {{discount_pct}}% ({{discount_health}})
- Risk flag: {{risk_flag}}

Tasks:
1. Identify the primary revenue driver
   (ORDER_VOLUME, PROMOTION, PRICE_MIX, OPERATIONAL_ISSUE, UNKNOWN)
2. Write a 3-5 sentence executive business summary
3. If risk_flag is true, clearly explain the risk
Return output as JSON with fields:
- date
- revenue_driver
- executive_summary



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

df = spark.createDataFrame([agent_result])
df.display()
df.printSchema()


# COMMAND ----------

import json
import re

def call_llm(llm_input: dict) -> dict:

    prompt = f"""
You are a senior business analyst.

Given the following daily metrics, do the tasks below.
Do NOT invent numbers. Use only the data provided.

Data:
- Date : {llm_input['date']}
- Net revenue change (%): {llm_input['net_revenue_wow_pct']}
- Orders change (%): {llm_input['orders_wow_pct']}
- Payment success rate: {llm_input['payment_success_rate']} ({llm_input['payment_health']})
- Refund rate: {llm_input['refund_rate']} ({llm_input['refund_risk']})
- Discount usage: {llm_input['discount_pct']}% ({llm_input['discount_health']})
- Risk flag: {llm_input['risk_flag']}

Tasks:
1. Identify the primary revenue driver
   (ORDER_VOLUME, PROMOTION, PRICE_MIX, OPERATIONAL_ISSUE, UNKNOWN)
2. Write a 3-5 sentence executive business summary
3. If risk_flag is true, clearly explain the risk

Return output as JSON with fields:
- date
- revenue_driver
- executive_summary
"""

    response = openai_client.chat.completions.create(
        model=LLM_ENDPOINT_NAME,
        messages=[{"role": "user", "content": prompt}]
    )

    raw_content = response.choices[0].message.content.strip()

    # ---- JSON extraction (your logic, unchanged) ----
    json_match = re.search(r'```json(.*?)```', raw_content, re.DOTALL)
    if not json_match:
        json_match = re.search(r'```(.*?)```', raw_content, re.DOTALL)

    if json_match:
        json_str = json_match.group(1).strip()
    else:
        json_str = raw_content

    if not json_str:
        raise ValueError("Empty LLM response")

    agent_result = json.loads(json_str)

    return agent_result


# COMMAND ----------

rows = signals.collect()

insight_rows = []

for row in rows:

    llm_input = {
        "date": row["order_date"],
        "net_revenue_wow_pct": row["net_revenue_wow_pct"],
        "orders_wow_pct": row["orders_wow_pct"],
        "payment_success_rate": row["payment_success_rate"],
        "payment_health": row["payment_health"],
        "refund_rate": row["refund_rate"],
        "refund_risk": row["refund_risk"],
        "discount_pct": row["discount_pct"],
        "discount_health": row["discount_health"],
        "risk_flag": row["risk_flag"]
    }

    llm_output = call_llm(llm_input)

    insight_rows.append(llm_output)
    # print(row)


# COMMAND ----------

business_daily_insights_df = spark.createDataFrame(insight_rows)
business_daily_insights_df.display()

# COMMAND ----------

final_business_daily_insights = (
    signals
    .join(business_daily_insights_df, signals.order_date == business_daily_insights_df.date)
    .select(
        F.col("order_date").alias("insight_date"),
        "net_revenue",
        "net_revenue_wow_pct",
        F.col("total_orders").alias("orders_count"),
        "orders_wow_pct",
        "payment_success_rate",
        "payment_health",
        "refund_rate",
        "refund_risk",
        "discount_pct",
        "discount_health",
        "revenue_driver",
        "executive_summary",
        "risk_flag",
        F.current_timestamp().alias("generated_at")
    )
)

# COMMAND ----------

final_business_daily_insights.display()

# COMMAND ----------

final_business_daily_insights.write.mode("overwrite").saveAsTable("agentic_ai_catalog.agent_test.agent_business_daily_insights")