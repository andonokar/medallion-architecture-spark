from pyspark.sql import SparkSession
import os
acess_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')


class SparkInit:
    def __init__(self):
        """
        Configurations for the spark environment
        Avaliar disponibilidade da máquina:
        boas práticas:
            usar 75% da memoria da maquina para o driver
            se mais de 4 cores, deixar 1 livre para a maquina
            configurar memory overhead para um pouco mais de 10% da memoria do driver
            path ivy arrumar para necessidades do docker
        """
        builder = SparkSession.builder \
            .config("spark.driver.memory", "10g") \
            .config("spark.driver.memoryOverhead", "3g") \
            .config("spark.shuffle.file.buffer", "1m") \
            .config("spark.file.transferTo", "false") \
            .config("spark.shuffle.unsafe.file.output.buffer", "1m") \
            .config("spark.io.compression.lz4.blockSize", "512k") \
            .config("spark.shuffle.service.index.cache.size", "100m") \
            .config("spark.shuffle.registration.timeout", "120000ms") \
            .config("spark.shuffle.registration.maxAttempts", "5") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "2047m") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.access.key", acess_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

        self.spark_session = builder.getOrCreate()

    def get_spark_session(self):
        return self.spark_session

# .config("spark.jars.ivy", "/mnt/c/programas/spark-core-repo/ivyjars") \
# .config("spark.jars.packages",
#         "io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-avro_2.12:3.3.2,"
#         "com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.apache.hadoop:hadoop-common:3.3.2,"
#         "org.apache.hadoop:hadoop-aws:3.3.2") \
