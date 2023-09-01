from delta.tables import DeltaTable  # Delta library
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from functools import reduce  # Reduction Operations
#from abc import ABC, abstractmethod  # Building an Abstract class


class ProcessingDeltaTable():
    """
      This class provides methods for to execute transform operations at delta tables

      Attributes:
          delta_table_path: delta table path
          delta_table_name: delta table name
          spark: spark session
          dataframe: dataframe that will be used to execute the transformation data

      Methods:
          get_delta_table: Returns the area of a rectangle.
          read_delta_table: Returns the perimeter of a rectangle.
          rename_columns_by_list(self, col_names_from: list[str], col_names_to: list[str]): Returns the area of a circle.
          filter_data(self, sql: str): Returns the circumference of a circle.
          rename_columns_by_dict(self, columns: dict):
          sql_execute(self, sql_query: str, table_tmp: str):
          create_new_delta_table(self, full_path: str, type_table: str = 'delta',
                               type_of_schema: str = 'overwriteSchema',
                               is_schema: str = 'true'):
          union_data_frame(self, df_list: list[DataFrame]):
  """

    def __init__(
            self,
            delta_table_path,
            delta_table_name,
            spark

    ):
        """

        :param delta_table_path:
        :param delta_table_name:
        :param spark:
        """

        self.spark = spark
        self.delta_table_path = delta_table_path
        self.delta_table_name = delta_table_name
        self.delta_table_full_name = self.delta_table_path + self.delta_table_name
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.dataframe = None




    def rename_columns_by_list(self, col_names_from: list[str], col_names_to: list[str]):
        """

        :param df:
        :param col_names_from:
        :param col_names_to:
        :return:
        """
        if isinstance(col_names_from, list) and isinstance(col_names_to, list) and isinstance(col_names_from) \
                and len(col_names_to) == len(col_names_from):
            mapping = dict(zip(col_names_from, col_names_to))
            self.dataframe.select([F.col(c).alias(mapping.get(c, c)) for c in self.dataframe.columns])
        else:
            raise ValueError(f" columns arrays: {col_names_from} <> {col_names_from} in length")

    def filter_data(self, sql: str):
        """

        :param sql:
        :return:
        """
        self.dataframe = self.dataframe.filter(sql)

    def rename_columns_by_dict(self, columns: dict):
        """

        :param columns:
        :return:
        """
        if isinstance(columns, dict):
            self.dataframe.select(*[F.col(col_name).alias(columns.get(col_name, col_name))
                                    for col_name in self.dataframe.columns])
        else:
            raise ValueError(f"'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")



    def union_data_frame(self, df_list: list[DataFrame]):
        """
        Une vários dataframes tonando-os apenas um
        :param df_list:
        :return:
        """
        df_reduce = reduce(DataFrame.unionAll, df_list)
        self.dataframe.unionAll(df_reduce)

    # @abstractmethod
    def run(self):
        """

        :return:
        """
        pass

        # def write_to_silver(
    #                         self,
    #                         prefix: str,
    #                         sql: str,
    #                         upsert: bool,
    #                         comparative_keys:str = None,
    #                         insert_condition:str = None,
    #                         update_condition:str = None,
    #                         delete_condition:str = None,
    #                     ):
    #
    #     df = self.spark.read.load(f"s3a://{self.bronze_bucket}/{prefix}")
    #     table = prefix.split("/")[-1]
    #     df.createOrReplaceTempView(table)
    #     df = self.spark.sql(sql)
    #
    #     if upsert:
    #         silver_data = DeltaTable.forPath(self.spark,f"s3a://{self.silver_bucket}/{prefix}")
    #
    #         if delete_condition:
    #             print(f"Proceeding with delete condition = {delete_condition}")
    #             delete_condition = upsert["delete"]
    #             (
    #             silver_data.alias("s")
    #                 .merge(df.alias("d"), comparative_keys)
    #                 .whenMatchedDelete(condition = delete_condition)
    #                 .whenMatchedUpdateAll(condition = update_condition)
    #                 .whenNotMatchedInsertAll(condition = insert_condition)
    #                 .execute()
    #             )
    #
    #         else:
    #             print("Proceesing with no delete condition")
    #
    #             (
    #             silver_data.alias("s")
    #                 .merge(df.alias("d"), comparative_keys)
    #                 .whenMatchedUpdateAll(condition = update_condition)
    #                 .whenNotMatchedInsertAll(condition = insert_condition)
    #                 .execute()
    #             )
    #     else:
    #         df.write.mode('overwrite').format("delta").save(f"s3a://{self.silver_bucket}/{prefix}")
    #
    #         deltaTable = DeltaTable.forPath(self.spark, f"s3a://{self.silver_bucket}/{prefix}")
    #         deltaTable.generate("symlink_format_manifest")
    #
    #
    # def write_to_gold(
    #                         self,
    #                         prefix_list: list,
    #                         prefix: str,
    #                         sql: str,
    #                         upsert: bool,
    #                         comparative_keys:str = None,
    #                         insert_condition:str = None,
    #                         update_condition:str = None,
    #                         delete_condition:str = None,
    #                     ):
    #
    #     for p in prefix_list:
    #         df = self.spark.read.load(f"s3a://{self.silver_bucket}/{p}")
    #         table = p.split("/")[-1]
    #         df.createOrReplaceTempView(table)
    #
    #     df = self.spark.sql(sql)
    #
    #     if upsert:
    #         gold_data = DeltaTable.forPath(self.spark, f"s3a://{self.gold_bucket}/{prefix}")
    #
    #         if delete_condition:
    #             print(f"Proceeding with delete condition = {delete_condition}")
    #             delete_condition = upsert["delete"]
    #             (
    #             gold_data.alias("g")
    #                 .merge(df.alias("d"), comparative_keys)
    #                 .whenMatchedDelete(condition = delete_condition)
    #                 .whenMatchedUpdateAll(condition = update_condition)
    #                 .whenNotMatchedInsertAll(condition = insert_condition)
    #                 .execute()
    #             )
    #
    #         else:
    #             print("Proccesing with no delete condition")
    #
    #             (
    #             gold_data.alias("g")
    #                 .merge(df.alias("d"), comparative_keys)
    #                 .whenMatchedUpdateAll(condition = update_condition)
    #                 .whenNotMatchedInsertAll(condition = insert_condition)
    #                 .execute()
    #             )
    #     elif not upsert:
    #         df.write.mode('overwrite').format("delta").save(f"s3a://{self.gold_bucket}/{prefix}")
    #
    #         deltaTable = DeltaTable.forPath(self.spark, f"s3a://{self.gold_bucket}/{prefix}")
    #         deltaTable.generate("symlink_format_manifest")
