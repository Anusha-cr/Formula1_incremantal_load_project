# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def write_to_silver(
    input_df,
    target_table,
    merge_condition,
    columns_to_update
):
    
    final_df = (
        input_df
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    if not spark.catalog.tableExists(target_table):
        (
            final_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
    else:
        delta_table = DeltaTable.forName(spark, target_table)
        update_map = {column: f"s.{column}" for column in columns_to_update}
        update_map["updated_at"] = "s.updated_at"
        target_columns = set(spark.table(target_table).columns)

        merge_builder = (
            delta_table.alias("t")
            .merge(
                final_df.alias("s"),
                merge_condition
            )
        )

        if "batch_id" in target_columns:
            merge_builder = merge_builder.whenMatchedUpdate(
                condition="s.batch_id >= t.batch_id",
                set=update_map
            )
        else:
            merge_builder = merge_builder.whenMatchedUpdate(
                set=update_map
            )

        (
            merge_builder
            .whenNotMatchedInsertAll()
            .execute()
        )