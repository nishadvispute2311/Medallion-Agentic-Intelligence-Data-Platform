import dlt

@dlt.table(name="bronze_orders")
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("header","true")
            .option("cloudFiles.inferColumnTypes","true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/agentic_ai_catalog/agent_test/raw_csv_data/_schemas/orders")
            .load("/Volumes/agentic_ai_catalog/agent_test/raw_csv_data/orders/")
    )


@dlt.table(name="bronze_customers")
def bronze_customers():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("header","true")
            .option("cloudFiles.inferColumnTypes","true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/agentic_ai_catalog/agent_test/raw_csv_data/_schemas/customers")
            .load("/Volumes/agentic_ai_catalog/agent_test/raw_csv_data/customers/")
    )


@dlt.table(name="bronze_payments")
def bronze_payments():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("header","true")
            .option("cloudFiles.inferColumnTypes","true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/agentic_ai_catalog/agent_test/raw_csv_data/_schemas/payments")
            .load("/Volumes/agentic_ai_catalog/agent_test/raw_csv_data/payments/")
    )


@dlt.table(name="bronze_refunds")
def bronze_refunds():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("header","true")
            .option("cloudFiles.inferColumnTypes","true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/agentic_ai_catalog/agent_test/raw_csv_data/_schemas/refunds")
            .load("/Volumes/agentic_ai_catalog/agent_test/raw_csv_data/refunds/")
    )