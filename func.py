"""
Function level adapters
"""
import os
import pendulum
from univariate.duplicate import DuplicateProcessor, DuplicateReport
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
import logging

__all__ = [
    "get_conf_from_evn",
    "parse_spark_extra_conf",
    "load_validated_data",
    "deduplicate",
    "save_dedup_data_to_dwh",
]

logger = logging.getLogger()


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Feature id
        conf["FEATURE_ID"] = os.getenv("FEATURE_ID")
        # Metadata
        conf["METADATA_HOST"] = os.getenv("METADATA_HOST")
        conf["METADATA_PORT"] = os.getenv("METADATA_PORT")
        conf["METADATA_TYPE"] = os.getenv("METADATA_TYPE", default="sensor")
        conf["METADATA_BACKEND"] = os.getenv("METADATA_BACKEND", default="MongoDB")
        # Data source
        conf["SOURCE_HOST"] = os.getenv("SOURCE_HOST")
        conf["SOURCE_PORT"] = os.getenv("SOURCE_PORT")
        conf["SOURCE_DATA_PATH_PREFIX"] = os.getenv(
            "SOURCE_DATA_PATH_PREFIX", default=""
        )
        conf["SOURCE_BACKEND"] = os.getenv("SOURCE_BACKEND", default="HDFS")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  # yyyy-MM-dd'T'HH:mm:ss
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE", default="UTC")

        conf["SPARK_EXTRA_CONF_PATH"] = os.getenv(
            "SPARK_EXTRA_CONF_PATH", default=""
        )  # [AICNS-61]
        conf["start"] = pendulum.parse(start_datetime, tz=conf["APP_TIMEZONE"])
        conf["end"] = pendulum.parse(end_datetime, tz=conf["APP_TIMEZONE"])

    except Exception as e:
        print(e)
        raise e
    return conf


def parse_spark_extra_conf(app_conf):
    """
    Parse spark-default.xml style config file.
    It is for [AICNS-61] that is spark operator take only spark k/v confs issue.
    :param app_conf:
    :return: Dict (key: conf key, value: conf value)
    """
    with open(app_conf["SPARK_EXTRA_CONF_PATH"], "r") as cf:
        lines = cf.read().splitlines()
        config_dict = dict(
            list(
                filter(
                    lambda splited: len(splited) == 2,
                    (map(lambda line: line.split(), lines)),
                )
            )
        )
    return config_dict


def load_validated_data(app_conf, time_col_name, data_col_name) -> DataFrame:
    """
    Validated data from DWH(Hive)
    :param app_conf:
    :param feature:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    table_name = "validated"
    # Inconsistent cache
    # https://stackoverflow.com/questions/63731085/you-can-explicitly-invalidate-the-cache-in-spark-by-running-refresh-table-table
    SparkSession.getActiveSession().sql(f"REFRESH TABLE {table_name}")
    query = f'''
        SELECT {time_col_name}, {data_col_name}  
            FROM  {table_name}
                WHERE feature_id = {app_conf["FEATURE_ID"]} 
                AND {time_col_name} >= {app_conf['start'].int_timestamp * 1000} 
                AND {time_col_name} < {app_conf['end'].int_timestamp * 1000}
        '''
    logger.info("load_validated_data query: " + query)
    ts = SparkSession.getActiveSession().sql(query)
    logger.info(ts.show())
    return ts.sort(F.col(time_col_name).desc())


def deduplicate(ts: DataFrame, time_col_name: str, data_col_name: str) -> DataFrame:
    """

    :param ts:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    logger.info("before dedup data count: " + str(ts.count()))
    report: DuplicateReport = DuplicateProcessor.detect_duplicates(ts=ts, time_col_name=time_col_name, data_col_name=data_col_name)
    # todo: Not none, but empty df
    logger.info("duplicated record count: " + str(report.duplicate_df.count()) if report.duplicate_df is not None else 0)
    # todo: propagate report
    dedup_df = DuplicateProcessor.drop_duplicates(ts=ts)
    logger.info("after dedup data count: " + str(dedup_df.count()))
    return dedup_df


def append_partition_cols(ts: DataFrame, feature_id: str, time_col_name: str, data_col_name, tz):
    SparkSession.getActiveSession().conf.set("spark.sql.session.timeZone", tz)
    partitioned = ts.withColumn("datetime", F.from_unixtime(F.col(time_col_name) / 1000)) \
        .select(
            time_col_name,
            data_col_name,
            F.lit(feature_id).alias("feature_id"),
            F.year("datetime").alias("year"),
            F.month("datetime").alias("month"),
            F.dayofmonth("datetime").alias("day"),
        ) \
        .sort(time_col_name)
    SparkSession.getActiveSession().conf.unset("spark.sql.session.timeZone")
    return partitioned


def save_dedup_data_to_dwh(
    ts: DataFrame, app_conf, time_col_name: str, data_col_name: str
):
    """
    Upsert data
    :param ts:
    :param stage:
    :param app_conf:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    # todo: transaction
    table_name = "cleaned_deduplicated"
    SparkSession.getActiveSession().sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({time_col_name} BIGINT, {data_col_name} DOUBLE) PARTITIONED BY (feature_id CHAR(10), year int, month int, day int) STORED AS PARQUET"
    )
    period = pendulum.period(app_conf["start"], app_conf["end"].subtract(days=1))

    # Create partition columns(year, month, day) from timestamp
    partition_df = append_partition_cols(ts, app_conf["FEATURE_ID"], time_col_name, data_col_name, app_conf["APP_TIMEZONE"])

    for date in period.range("days"):
        # Drop Partition for immutable task
        SparkSession.getActiveSession().sql(
            f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION(feature_id={app_conf['FEATURE_ID']}, year={date.year}, month={date.month}, day={date.day})"
        )
    # Save
    partition_df.write.format("hive").mode("append").insertInto(table_name)
