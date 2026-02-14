import dlt
from pyspark.sql.functions import *

# ==============================
# KPI 1: Daily Order Revenue KPI
# ==============================
@dlt.table(
    name="kpi_daily_order_revenue",
    comment="Daily revenue, orders count, net revenue"
)
def kpi_daily_order_revenue():
    fact = dlt.read("fact_orders")
    return (
        fact
        .groupBy(to_date("order_date").alias("order_date"))
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("payment_amount").alias("gross_revenue"),
            sum("net_amount").alias("net_revenue")
        )
    )


# ==============================
# KPI 2: Payment Success Rate KPI
# ==============================
@dlt.table(
    name="kpi_payment_success_rate",
    comment="Payment success vs failure trend"
)
def kpi_payment_success_rate():
    fact = dlt.read("fact_orders")
    return (
        fact
        .groupBy(to_date("order_date").alias("order_date"))
        .agg(
            count("payment_id").alias("total_payments"),
            sum(when(col("payment_status") == "SUCCESS", 1).otherwise(0)).alias("successful_payments"),
            round(
                sum(when(col("payment_status") == "SUCCESS", 1).otherwise(0)) / count("payment_id") * 100,
                2
            ).alias("payment_success_rate")
        )
    )


# ==============================
# KPI 3: Refund Rate KPI
# ==============================
@dlt.table(
    name="kpi_refund_rate",
    comment="Refund rate and refund amount tracking"
)
def kpi_refund_rate():
    fact = dlt.read("fact_orders")
    return (
        fact
        .groupBy(to_date("order_date").alias("order_date"))
        .agg(
            countDistinct("order_id").alias("total_orders"),
            count("refund_id").alias("refunded_orders"),
            sum("refund_amount").alias("total_refund_amount"),
            round(count("refund_id") / countDistinct("order_id") * 100, 2).alias("refund_rate")
        )
    )


# ==============================
# KPI 4: Customer Lifetime Value KPI
# ==============================
@dlt.table(
    name="kpi_customer_lifetime_value",
    comment="Customer lifetime value and order frequency"
)
def kpi_customer_lifetime_value():
    fact = dlt.read("fact_orders")
    return (
        fact
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("net_amount").alias("lifetime_value"),
            avg("net_amount").alias("avg_order_value")
        )
    )


# ==============================
# KPI 5: Payment Method Performance KPI
# ==============================
@dlt.table(
    name="kpi_payment_method_performance",
    comment="Payment method success & revenue contribution"
)
def kpi_payment_method_performance():
    fact = dlt.read("fact_orders")
    return (
        fact
        .groupBy("payment_method")
        .agg(
            count("payment_id").alias("total_transactions"),
            sum(when(col("payment_status") == "SUCCESS", 1).otherwise(0)).alias("successful_transactions"),
            sum("net_amount").alias("total_revenue")
        )
    )


# ==============================
# KPI 6: Order Discount Impact KPI
# ==============================
@dlt.table(
    name="kpi_discount_impact",
    comment="Discount impact on revenue"
)
def kpi_discount_impact():
    fact = dlt.read("fact_orders")
    return (
        fact
        .groupBy(to_date("order_date").alias("order_date"))
        .agg(
            sum("discount_amount").alias("total_discount"),
            sum("payment_amount").alias("gross_revenue"),
            sum("net_amount").alias("net_revenue")
        )
    )
