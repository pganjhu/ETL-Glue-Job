# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 11:19:25 2023

@author: pkg
"""

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, coalesce
from pyspark.sql import SQLContext
from pyspark.sql import Row
import time

# Source MS SQL Server Connection details

source_conn = {
    "url": "host_url",
    "db": "database_name",
    "table":"table_name"
}

db_properties = {
    "username": "user_name",
    "password": "password"
}

def extract_FULL_sourcedata(spark):
    
    # Reading data from Source : EVOLVE MS SQL Server
    
    x = time.time()
    df = spark.read \
        .format("jdbc") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("url","jdbc:sqlserver://;serverName="+source_conn["url"]+";DATABASE="+source_conn["db"]) \
        .option("user",db_properties["username"]) \
        .option("password",db_properties["password"]) \
        .option("dbtable","schema_name.table_name") \
        .load() 
    
    print("Time taken to read data ",time.time()-x)

    print("Record Counts :",df.count())
    df.printSchema()
    df.show()
    
    # Writing above dataframe to S3 
    x = time.time()
    df.write.parquet("s3://bucket_name/sub_path",mode="overwrite")
    print("Time taken to write to S3 ",time.time()-x)
    


if __name__ == "__main__":
    glueContext = GlueContext(SparkContext.getOrCreate())
    glueJob = Job(glueContext)
    spark = glueContext.sparkSession

    extract_FULL_sourcedata(spark)

    glueJob.commit()
