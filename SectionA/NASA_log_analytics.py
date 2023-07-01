import re
import glob
from datetime import datetime
import pandas as pd
from pyspark.sql import functions as F

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import date_format, regexp_extract, udf, row_number
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.window import Window


ss = SparkSession.builder.getOrCreate()
sc = ss.sparkContext


def get_day_of_week(date_string):
    """
    Convert date string to day of week
    """

    if date_string:
        date_obj = datetime.strptime(date_string, "%d/%b/%Y")
        return date_obj.strftime("%A")
    else:
        return "Unknown"


def get_timestamp(date_string):
    """
    Convert date string to timestamp
    """
    if date_string:
        date_obj = datetime.strptime(date_string, "%d/%b/%Y:%H:%M:%S")
        return date_obj.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return "Unknown"


def create_logs_df():
    """
    Create a dataframe from the raw data
    """
    raw_data_files = glob.glob("*.gz")
    base_df = ss.read.text(raw_data_files)

    ts_pattern = r"\[(\d{2}/\w{3}/\d{4})"  # 01/Jul/1995
    # 01/Jul/1995:00:00:01
    ts_pattern_2 = r"\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})"
    # "GET /shuttle/missions/sts-71/mission-sts-71.html HTTP/1.0"
    method_uri_protocol_pattern = r"\"(\S+)\s(\S+)\s*(\S*)\""
    status_pattern = r"\s(\d{3})\s"  # 200
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
    content_size_pattern = r'\s(\d+)$'

    logs_df = base_df.select(
        udf(get_day_of_week)(regexp_extract("value", ts_pattern, 1)).alias(
            "Day in a week"),
        udf(get_timestamp)(regexp_extract(
            "value", ts_pattern_2, 1)).alias("timestamp"),
        regexp_extract("value", host_pattern, 1).alias("host"),
        # regexp_extract("value", ts_pattern, 1).alias("timestamp"),
        regexp_extract('value', method_uri_protocol_pattern,
                       1).alias("method"),
        regexp_extract('value', method_uri_protocol_pattern,
                       2).alias("endpoint"),
        regexp_extract('value', method_uri_protocol_pattern,
                       3).alias("protocol"),
        regexp_extract("value", status_pattern, 1).cast(
            "integer").alias("status"),
        regexp_extract('value', content_size_pattern, 1).cast(
            "integer").alias("content_size")
    )
    return logs_df


def count_null(df):
    """
    count the number of rows with null values
    """
    bad_rows_df = df.filter(df['host'].isNull() |
                            df['timestamp'].isNull() |
                            df['method'].isNull() |
                            df['endpoint'].isNull() |
                            df['status'].isNull() |
                            df['content_size'].isNull() |
                            df['protocol'].isNull())

    return bad_rows_df.count()


def count_null_col(col_name):
    """
    Count the number of null values for a specific column.
    """
    return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)


def count_null_cols(df):
    """
    Count the number of null values for each column.
    """
    col = df.columns
    exprs = [count_null_col(col_name) for col_name in col]
    return df.agg(*exprs)


def fill_null(df):
    """
    Fill null values in the content_size column with 0.
    """
    return df.na.fill({'content_size': 0})


def parse_clf_time(text):
    """ Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
        'Aug': 8,  'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    if text:
        return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
            int(text[7:11]),
            month_map[text[3:6]],
            int(text[0:2]),
            int(text[12:14]),
            int(text[15:17]),
            int(text[18:20])
        )
    else:
        return "Unknown"


def process_timestamp(df):
    """
    Convert three-character month expression to an integer and format timestamp string to timestamp
    """
    udf_parse_time = udf(parse_clf_time)

    df = df.select('host',
                   udf_parse_time(df['timestamp']).alias('timestamp'),
                   'method', 'endpoint', 'protocol', 'status', 'content_size')

    df = df.select('*',
                   df['timestamp'].cast('timestamp').alias('time')).drop('timestamp')

    return df


def endpoint_invocations_by_day(logs_df):
    logs_df.createOrReplaceTempView("invocations")

    query = """
    SELECT `Day in a week`, endpoint, COUNT(*) AS count
    FROM invocations
    WHERE `Day in a week` != 'Unknown'
    GROUP BY `Day in a week`, endpoint
    """
    result = ss.sql(query)

    windowSpec = Window.partitionBy(
        "Day in a week").orderBy(result["count"].desc())
    top_invocation = (
        result.withColumn("rn", row_number().over(windowSpec))
        .where("rn == 1")
        .drop("rn")
        .cache()
    )

    return top_invocation


def status404_Count_by_Day(logs_df):
    logs_df.createOrReplaceTempView("invocations")

    query = """
    SELECT `Day in a week`, status, COUNT(*) AS count
    FROM invocations
    WHERE status == 404
    GROUP BY `Day in a week`, status
    ORDER BY count DESC
    """
    result = ss.sql(query)

    return result


def status_Count_by_Day(logs_df):
    logs_df.createOrReplaceTempView("invocations")

    query = """
    SELECT status, COUNT(*) AS count
    FROM invocations
    GROUP BY status
    ORDER BY count DESC
    """
    result = ss.sql(query)

    return result


def top_host(logs_df):
    logs_df.createOrReplaceTempView("invocations")

    query = """
    SELECT host, COUNT(*) AS count
    FROM invocations
    GROUP BY host
    ORDER BY count DESC
    """
    result = ss.sql(query)

    return result


def top_10_endpoint(logs_df):
    logs_df.createOrReplaceTempView("invocations")

    query = """
    SELECT endpoint, COUNT(*) AS count
    FROM invocations
    GROUP BY endpoint
    ORDER BY count DESC
    LIMIT 10
    """
    result = ss.sql(query)

    return result


def aggregation(logs_df):
    (logs_df.agg(F.min(logs_df['content_size']).alias('min_content_size'),
                 F.max(logs_df['content_size']).alias('max_content_size'),
                 F.mean(logs_df['content_size']).alias('mean_content_size'),
                 F.stddev(logs_df['content_size']).alias('std_content_size'),
                 F.count(logs_df['content_size']).alias('count_content_size'))
        .toPandas())
    return logs_df
