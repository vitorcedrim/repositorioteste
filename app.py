from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark as pyspark

import json
import re
import pandas as pd

config = None
spark_session = None

def createSparkSession(app_name, export_format):
    match export_format.upper():
        case "BIGQUERY":
            print("Configuring a Spark Session for BigQuery export")
            return (SparkSession.builder.appName(app_name)
            #.config("spark.jars", "spark-3.4-bigquery-0.34.0.jar") > Use if environment does not contains BigQuery connector by default.
            .getOrCreate())
        case _:
            print(f"Configuring a Spark Session for local file export ({export_format})")
            return (SparkSession.builder.appName(app_name)
            .getOrCreate())

def loadConfigFile(config_file_path):
    try:
        sc = pyspark.SparkContext.getOrCreate()
        data_collected = json.loads(''.join(sc.textFile(config_file_path).collect()))
        sc.stop()
        return data_collected
    except:
        raise ValueError("Config file not found. Please check the path and try again.")
    

def loadData(file_type, file_path):
    match file_type.upper():
        case "CSV":
            #spark.read.csv(GCS_FILE_PATH, header=True, inferSchema=True)
            return spark_session.read.csv(file_path, header=True, inferSchema=True)
        case "PARQUET":
            return spark_session.read.parquet(file_path)
        case _:
            raise ValueError("Unsupported file type. Please use CSV or PARQUET.")


def aggregateDataFromColumnList(df, aggregation_list, aggregation_type):
    match aggregation_type.upper():
        case "COUNT":
            return df.groupBy(*aggregation_list).count()
        case "SUM":
            return df.groupBy(*aggregation_list).sum()
        case "AVG":
            return df.groupBy(*aggregation_list).avg()
        case "MAX":
            return df.groupBy(*aggregation_list).max()
        case "MIN":
            return df.groupBy(*aggregation_list).min()
        case _:
            raise ValueError("Unsupported aggregation type. Please use COUNT, SUM, AVG, MAX, or MIN.")
        
def normalizeColumnNames(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, column.upper()) # All uppercase
        df = df.withColumnRenamed(column, column.replace("(", "").replace(")", "")) # Remove parenthesis
        df = df.withColumnRenamed(column, column.replace(":", "")) # Remove double dots
        df = df.withColumnRenamed(column, column.replace(" ", "_")) # Change spaces to underscore
        df = df.withColumnRenamed(column, column.replace("/", "_")) # Change slashes to underscore
    
    return df

def extractDomainFromURL(df, url_column_names):
    def extractionLogic(url):
        if url:
            pattern = r"(?:https?:\/\/)?(?:www\.)?([^\/]+)"
            match = re.search(pattern, url)
            if match:
                return match.group(1)
            return None

    for name in url_column_names:
        extraction_udf = udf(extractionLogic, StringType())
        df = df.withColumn(f"{name}", extraction_udf(df[f"{name}"]))

    return df

def convertDate(df, column_name):
    df = df.withColumn(column_name, from_unixtime(col(f"{column_name}") / 1000000).cast("date"))
    return df

def output_file(df, output_format, output_path):
    match output_format.upper():
        case "CSV":
            df.toPandas().to_csv(output_path + ".csv", index=False)
        case "PARQUET":
            df.toPandas().to_parquet(output_path + ".parquet", index=False)
        case _:
            return None

# ---> ORCHESTRATOR FUNCTION <---
def modelData(df):
    df = convertDate(df, config["time_column_name"])
    df = extractDomainFromURL(df, config["url_column_names"])
    df = aggregateDataFromColumnList(df, config["aggregate_list"], config["aggregation_type"])
    df = normalizeColumnNames(df)

    return df
        
# ---> MAIN CODE <---
config = loadConfigFile("config.json")
spark_session = createSparkSession(config["spark_app_name"], config["output_format"])
spark_session
raw_df = loadData(config["data_file_type"], config["data_file_path"])
mod_df = modelData(raw_df)

match config["output_format"].upper():
    case "BIGQUERY":
        try:
            (mod_df.write.format("bigquery")
                .option("temporaryGcsBucket", f"{config["TEMP_BUCKET_NAME"]}")
                .option("table", f"{config["PROJECT_ID"]}:{config["DATASET_ID"]}.{config["TABLE_ID"]}")
                .mode(f"{config["MODE"]}")
                .save()
            )
            print("BigQuery export successful.")
        except Exception as e:
            print("BigQuery export failed. Please check your configuration and try again.")
            raise e
    case _:
        try:
            output_file(mod_df, config["output_format"], config["output_path"])
            print("Local file export successful.")
        except:
            print("Local file export failed. Please check your configuration and try again.")

#End Pyspark session to free up resources.
spark_session.stop()

#TODO: Add BigQuery export functionality.
#TODO: Make the aggregation function have the option to configure the type of aggegation. Not only "count". Aggregation type needs to be defined in JSON config.