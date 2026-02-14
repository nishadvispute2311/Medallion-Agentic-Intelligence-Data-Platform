import dlt
from pyspark.sql.functions import expr, lit, current_timestamp
import json

def read_guardian_control(table_name):
    row = (
        spark.table("agentic_ai_catalog.agent_test.gen_agent_control")
             .filter(f"table_name = '{table_name}'")
             .orderBy(expr("updated_at DESC"))
             .limit(1)
             .collect()
    )
    return json.loads(row[0]["fix_json"]) if row else None

def read_drift_control(table_name):
    row = (
        spark.table("agentic_ai_catalog.agent_test.drift_control2")
             .filter(f"table_name = '{table_name}'")
             .orderBy(expr("updated_at DESC"))
             .limit(1)
             .collect()
    )
    return json.loads(row[0]["agent_result_json"]) if row else None

def apply_corrections(df, correction_expression):
    for col, exp in correction_expression.items():
        df = df.withColumn(col, expr(exp))
    return df


def guardian_allows_processing(guardian_control):
    """
    Guardian blocks silver only when strategy = BLOCK
    """
    return guardian_control.get("strategy") != "BLOCK"


def drift_allows_processing(drift_control):
    """
    Drift blocks silver only when overall_severity = HIGH
    """
    return drift_control.get("overall_severity") != "HIGH"


def silver_allows_processing(guardian_control, drift_control):
    return (
        guardian_allows_processing(guardian_control)
        and
        drift_allows_processing(drift_control)
    )



@dlt.table(
    name="silver_customers",
    comment="Silver Customers with Guardian + Drift enforcement"
)
def silver_customers():

    TABLE_NAME = "bronze_customers"

    bronze_df = dlt.read("bronze_customers")

    guardian = read_guardian_control(TABLE_NAME)
    drift = read_drift_control(TABLE_NAME)

    # ----------------------------------
    #  FINAL GATE
    # ----------------------------------
    if not silver_allows_processing(guardian, drift):
        # DLT-safe: do NOT fail pipeline
        return bronze_df.limit(0)

    # ----------------------------------
    #  Guardian corrections
    # ----------------------------------
    if guardian["strategy"] in ("CORRECT", "BOTH"):
        bronze_df = apply_corrections(
            bronze_df,
            guardian["correction_expression"]
        )

    # ----------------------------------
    #  Guardian filtering
    # ----------------------------------
    silver_df = bronze_df.filter(
        guardian["silver_filter_condition"]
    )

    return (
        silver_df
        .withColumn("guardian_strategy", lit(guardian["strategy"]))
        .withColumn("drift_severity", lit(drift["overall_severity"]))
        .withColumn("processed_at", current_timestamp())
    )


@dlt.table(
    name="quarantine_customers",
    comment="Customers blocked by Guardian or Drift"
)
def quarantine_customers():

    TABLE_NAME = "bronze_customers"

    bronze_df = dlt.read("bronze_customers")

    guardian = read_guardian_control(TABLE_NAME)
    drift = read_drift_control(TABLE_NAME)

    # ----------------------------------
    # Guardian BLOCK → full quarantine
    # ----------------------------------
    if guardian["strategy"] == "BLOCK":
        return (
            bronze_df
            .withColumn("quarantine_reason", lit("GUARDIAN_BLOCK"))
            .withColumn("guardian_strategy", lit("BLOCK"))
            .withColumn("drift_severity", lit(drift["overall_severity"]))
            .withColumn("quarantined_at", current_timestamp())
        )

    # ----------------------------------
    # Drift HIGH → full quarantine
    # ----------------------------------
    if drift["overall_severity"] == "HIGH":
        return (
            bronze_df
            .withColumn("quarantine_reason", lit("DRIFT_HIGH_SEVERITY"))
            .withColumn("guardian_strategy", lit(guardian["strategy"]))
            .withColumn("drift_severity", lit("HIGH"))
            .withColumn("quarantined_at", current_timestamp())
        )

    # ----------------------------------
    # Guardian row-level quarantine
    # ----------------------------------
    failed_rows = bronze_df.filter(
        f"NOT ({guardian['silver_filter_condition']})"
    )

    return (
        failed_rows
        .withColumn("quarantine_reason", lit("GUARDIAN_FILTER_FAILED"))
        .withColumn("guardian_strategy", lit(guardian["strategy"]))
        .withColumn("drift_severity", lit(drift["overall_severity"]))
        .withColumn("quarantined_at", current_timestamp())
    )


