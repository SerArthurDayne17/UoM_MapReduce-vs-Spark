from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse

def transform_data(data_source: str, output_uri: str) -> None:
    with SparkSession.builder.appName("My First App").getOrCreate() as spark:
       
       #Load CSV Data
       df = spark.read.option("header", "true").csv(data_source)
       
       # Create an in memory DataFrame
       df.createOrReplaceTempView("DelayedFlights")

       #Construct SQL Query
       GROUP_BY_QUERY ="""
            SELECT Year, count(*)
            FROM DelayedFlights
            WHERE FlightNum = 1362
            GROUP BY Year
        """
       
       #Transform Data
       transformed_df = spark.sql(GROUP_BY_QUERY)

       #Log into EMR stdout
       print(f"Number of rows in SQL query: {transformed_df.count()}")

       #Write to Parquet
       transformed_df.write.mode("overwrite").parquet(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_source")
    parser.add_argument("--output_uri")
    args = parser.parse_args()
    transform_data(args.data_source, args.output_uri)
