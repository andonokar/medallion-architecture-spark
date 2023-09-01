# from pyspark.sql.functions import hash, concat
import spark_init
from delta import DeltaTable

if __name__ == "__main__":

    spark_start = spark_init.SparkInit()
    spark = spark_start.get_spark_session()

    df = spark.createDataFrame([(1, "John", "Doe", "2022-01-01"), (2, "Jane", "Doe", "2022-02-01")], ["id", "first_name", "last_name", "date"])

    df.show()

# Register the dataframe as a temporary view
    df.createOrReplaceTempView("temp_table")
    print(DeltaTable.isDeltaTable(spark, "/home/domrock/Downloads/parquet/CEMIG/adr2x"))

    # Convert the date column to a string using date_format function
    # string_df = spark.sql("SELECT id,first_name,last_name,date, CONCAT(id,first_name,last_name,date_format(to_date(date), 'yyyy-MM-dd')) AS hash FROM temp_table")
    string_df = spark.sql("SELECT id,first_name,last_name,date, base64(CONCAT(id,first_name,last_name,date)) AS _id FROM temp_table")
    # string_df = string_df.select(hash(string_df.hash).alias("hash_value"))

    # Show the resulting dataframe
    string_df.show()
