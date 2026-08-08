# Databricks notebook source
# helper functions to add the file metadata to ingestion source file and ingestion timestamp
from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return(
        df
        .withColumn('ingestion_timestamp' , F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
    )

