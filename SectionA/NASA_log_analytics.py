import re
import glob
from datetime import datetime
import pandas as pd

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import date_format, regexp_extract, udf, row_number
from pyspark.sql.window import Window


ss = SparkSession.builder.getOrCreate()
sc = ss.sparkContext


def get_day_of_week(date_string):
    if date_string:
        date_obj = datetime.strptime(date_string, "%d/%b/%Y")
        return date_obj.strftime("%A")
    else:
        return "Unknown"


def create_logs_df():
    raw_data_files = glob.glob("*.gz")
    base_df = ss.read.text(raw_data_files)

    ts_pattern = r"\[(\d{2}/\w{3}/\d{4})"
    method_uri_protocol_pattern = r"\"(\S+)\s(\S+)\s*(\S*)\""
    status_pattern = r"\s(\d{3})\s"

    logs_df = base_df.select(
        udf(get_day_of_week)(regexp_extract("value", ts_pattern, 1)).alias(
            "Day in a week"
        ),
        regexp_extract("value", method_uri_protocol_pattern, 2).alias("endpoint"),
        regexp_extract("value", status_pattern, 1).cast("integer").alias("status"),
    )
    return logs_df


def endpoint_invocations_by_day(logs_df):
    logs_df.createOrReplaceTempView("invocations")

    query = """
    SELECT `Day in a week`, endpoint, COUNT(*) AS count
    FROM invocations
    WHERE `Day in a week` != 'Unknown'
    GROUP BY `Day in a week`, endpoint
    """
    result = ss.sql(query)

    windowSpec = Window.partitionBy("Day in a week").orderBy(result["count"].desc())
    top_invocation = (
        result.withColumn("rn", row_number().over(windowSpec))
        .where("rn == 1")
        .drop("rn")
    )

    return top_invocation


def status404_Count_by_Day(logs_df):
    logs_df.createOrReplaceTempView("invocations")

    query = """
    SELECT `Day in a week`, status, COUNT(*) AS count
    FROM invocations
    WHERE status == 404
    GROUP BY `Day in a week`, status
    """
    result = ss.sql(query)

    return result
