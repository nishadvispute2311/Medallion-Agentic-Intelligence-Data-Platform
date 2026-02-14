import dlt
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType
from pyspark.sql.types import *

POLICY_TABLE = "agentic_ai_catalog.agent_test.policy_control"

policy_df = (
    spark.read.table(POLICY_TABLE)
    .filter(col("pipeline_name") == "silver_to_gold_pipeline")
    .orderBy(col("updated_timestamp").desc())
    .limit(1)
)

policy_schema = StructType([
    StructField("strategy", StringType()),
    StructField("row_rules", ArrayType(MapType(StringType(), StringType()))),
    StructField("metric_rules", ArrayType(MapType(StringType(), StringType())))
])

policy_row = (
    policy_df
    .select(from_json(col("policy_decision_json"), policy_schema).alias("policy"))
    .first()
)

policy_json = policy_row["policy"]
strategy = policy_json["strategy"]
row_rules = policy_json["row_rules"] or []
metric_rules = policy_json["metric_rules"] or []


def normalize_expression(expr_str):
    return (
        expr_str
        .replace("= SUCCESS", "= 'SUCCESS'")
        .replace("!= SUCCESS", "!= 'SUCCESS'")
    )

def apply_row_rules(df, table_name, rules):
    applicable_rules = [r for r in rules if r["table"] == table_name]

    valid_condition = " AND ".join([r["expression"] for r in applicable_rules])


    if valid_condition:
        valid_condition = " AND ".join([normalize_expression(r["expression"]) for r in applicable_rules]
)
        valid_df = df.filter(expr(valid_condition))
        quarantine_df = df.filter(f"NOT ({valid_condition})")
    else:
        valid_df = df
        quarantine_df = None

    return valid_df, quarantine_df



@dlt.table(
    name="gold_quarantine_refunds",
    comment="Refunds quarantined by policy agent"
)
def gold_quarantine_refunds():

    df = dlt.read("silver_refunds")
    _, quarantine_df = apply_row_rules(df, "refunds", row_rules)

    return quarantine_df if quarantine_df is not None else df.limit(0)

@dlt.table(
    name="gold_quarantine_orders",
    comment="Orders quarantined by policy agent"
)
def gold_quarantine_orders():

    df = dlt.read("silver_orders")
    _, quarantine_df = apply_row_rules(df, "orders", row_rules)

    return quarantine_df if quarantine_df is not None else df.limit(0)

@dlt.table(
    name="gold_quarantine_payments",
    comment="Payments quarantined by policy agent"
)
def gold_quarantine_payments():

    df = dlt.read("silver_payments")
    _, quarantine_df = apply_row_rules(df, "payments", row_rules)

    return quarantine_df if quarantine_df is not None else df.limit(0)

@dlt.table(
    name="gold_quarantine_customers",
    comment="Customers quarantined by policy agent"
)
def gold_quarantine_customers():

    df = dlt.read("silver_customers")
    _, quarantine_df = apply_row_rules(df, "customers", row_rules)

    return quarantine_df if quarantine_df is not None else df.limit(0)


@dlt.table(
    name="fact_orders",
    comment="Gold fact table built using Silver + Policy Agent rules"
)
def fact_orders():

    silver_orders    = dlt.read("silver_orders")
    silver_payments  = dlt.read("silver_payments")
    silver_refunds   = dlt.read("silver_refunds")
    silver_customers = dlt.read("silver_customers")

    orders_valid2, _    = apply_row_rules(silver_orders, "orders", row_rules)
    payments_valid2, _  = apply_row_rules(silver_payments, "payments", row_rules)
    refunds_valid2, _   = apply_row_rules(silver_refunds, "refunds", row_rules)
    customers_valid2, _ = apply_row_rules(silver_customers, "customers", row_rules)

    o = orders_valid2.alias("o")
    c = customers_valid2.alias("c")
    p = payments_valid2.alias("p")
    r = refunds_valid2.alias("r")

    return (
        o
        .join(c, col("o.customer_id") == col("c.customer_id"), "left")
        .join(p, col("o.order_id") == col("p.order_id"), "left")
        .join(r, col("o.order_id") == col("r.order_id"), "left")
        .select(
            col("o.order_id"),
            col("o.customer_id"),
            col("p.payment_id"),
            col("r.refund_id"),

            col("o.order_date"),
            col("o.order_status"),

            col("p.payment_status"),
            col("p.payment_method"),
            col("p.amount").alias("payment_amount"),

            col("r.refund_status"),
            col("r.refund_amount"),
            col("r.refund_reason"),

            col("o.discount_amount"),
            (col("p.amount") - col("o.discount_amount")).alias("net_amount"),

            current_timestamp().alias("processed_at")
        )
    )