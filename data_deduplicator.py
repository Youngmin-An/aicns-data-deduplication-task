"""
Deduplicater Task
# 1. Detect dup and eliminate
# 2. Save dedup data to DWH (Now Hive concrete)
# 3. todo: Propagate report
"""

from func import *
from pyspark.sql import SparkSession
from univariate.analyzer import AnalysisReport

if __name__ == "__main__":
    # Initialize app
    app_conf = get_conf_from_evn()

    SparkSession.builder.config(
        "spark.hadoop.hive.exec.dynamic.partition", "true"
    ).config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    # [AICNS-61]
    if app_conf["SPARK_EXTRA_CONF_PATH"] != "":
        config_dict = parse_spark_extra_conf(app_conf)
        for conf in config_dict.items():
            SparkSession.builder.config(conf[0], conf[1])

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    app_conf["sql_context"] = spark

    # Get feature metadata
    data_col_name = (
        "input_data"  # todo: metadata concern or strict validation column names
    )
    time_col_name = "event_time"

    # Load data  # todo: will validated data go dwh?
    ts = load_validated_data(app_conf, time_col_name, data_col_name)

    # Deduplicate
    dedup_df = deduplicate(ts=ts, time_col_name=time_col_name, data_col_name=data_col_name)

    # Save  to DWH
    save_dedup_data_to_dwh(
        ts=dedup_df, app_conf=app_conf, time_col_name=time_col_name, data_col_name=data_col_name
    )
    # todo: store offline report

    spark.stop()
