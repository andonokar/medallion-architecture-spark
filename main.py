import spark_init
from classes.client import Client
from variables import deparaconfig

if __name__ == "__main__":

    spark_start = spark_init.SparkInit()
    spark = spark_start.get_spark_session()

    for client, setup in deparaconfig.items():
        client_class = Client(client, setup, spark)
        client_class.process_delta_tables()

    spark.stop()
