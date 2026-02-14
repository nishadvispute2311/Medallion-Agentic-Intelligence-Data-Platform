# Databricks notebook source
# Databricks notebook source
datasets = [
    {
        "table_name" : "bronze_orders",
        "table_type" : "TRANSACTIONAL"
    },
    {
        "table_name" : "bronze_customers",
        "table_type" : "DIMENSION"
    },
    {
        "table_name" : "bronze_payments",
        "table_type" : "TRANSACTIONAL"
    },
    {
        "table_name" : "bronze_refunds",
        "table_type" : "TRANSACTIONAL"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_datasets", datasets)