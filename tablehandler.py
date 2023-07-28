from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from util import log


class TableHandler(object):
    """
    Instancia tabelas para operacoes de leitura, escrita, upsert
    """

    def __init__(self, spark: SparkSession, localpath=None) -> None:
        """

        :param spark: sessao spark
        :param localpath: quando na nuvem, nao passar valor
        """
        self.spark = spark
        self.__pyspark_format_available = ['csv', 'avro', 'json', 'parquet', 'delta']
        self.__bool_available = ["true", "false"]
        self.__pyspark_mode = ["append", "overwrite"]
        self._local_path = localpath
        self._deltatable = None

        if self._local_path:
            self.is_deltatable()

    def get_deltatable(self) -> DeltaTable:
        """
         This method verify if a deltable exist or not
         In case true a deltatable will be available
         Otherwise return a error message
        :return:  Deltatable
        """
        self._set_deltatable()
        return self._deltatable

    def set_deltatable_path(self, path: str) -> None:
        self._local_path = path

    def is_deltatable(self):
        """
        Make testes if the path has a delta table stored or not
        :return: True if it is a deltatable otherwise return false
        """

        is_table = DeltaTable.isDeltaTable(self.spark, self._local_path)
        if is_table:
            self._set_deltatable()

        return is_table

    def _set_deltatable(self):
        """
        set a deltatable if it exists
        otherwise return a exception
        :return: None
        """
        self._deltatable = DeltaTable.forPath(self.spark, self._local_path)

    def get_table(self, path: str, options: dict):
        """
        read files in many formats
        :param path: local where the file is
        :param options: Options list to read the content
        :return: dataframe as a table
        """
        fmsg = f'{TableHandler.__name__}.{self.get_table.__name__}'
        if options['format_in'] in self.__pyspark_format_available and \
                options['header'] in self.__bool_available and \
                options['inferSchema'] in self.__bool_available:

            table = self.spark.read.format(options['format_in']) \
                .option('inferSchema', options['inferSchema']) \
                .option('header', options['header']) \
                .load(path)
        else:
            log.createLogger(fmsg).error(f"Unsupported format {self.__pyspark_format_available}  or "
                                         f"header differ from {self.__bool_available} or "
                                         f"inferSchema differ from {self.__bool_available} ")
            raise ValueError(f"Unsupported format {self.__pyspark_format_available}  or "
                             f"header differ from {self.__bool_available} or "
                             f"inferSchema differ from {self.__bool_available} ")
        return table

    @log.logs
    def write_table(self, dataframe, path: str, options: dict) -> None:
        """
        Write a file in many formats
        :param dataframe: data to be saved
        :param path: local
        :param options: options a seach parameters to help the process
        :return: None
        """
        fmsg = f'{TableHandler.__name__}.{self.write_table.__name__}'
        if options['format_out'] in self.__pyspark_format_available and \
                options['header'] in self.__bool_available and \
                options['mode'] in self.__pyspark_mode:

            dataframe.write.format(options['format_out']) \
                .mode(options['mode']) \
                .option('header', options['header']) \
                .save(path)

        else:
            log.createLogger(fmsg).error(f"Unsupported format {self.__pyspark_format_available}  or "
                                         f"header differ from {self.__bool_available} or "
                                         f"mode differ from {self.__pyspark_mode}")
            raise ValueError(f"Unsupported format {self.__pyspark_format_available}  or "
                             f"header differ from {self.__bool_available} or "
                             f"mode differ from {self.__pyspark_mode}")

    @log.logs
    def upsert_deltatable(self, dataframe: DataFrame, label_origem: str,
                          label_destino: str, condupdate: str) -> None:
        """
             Upsert is a operation that insert a date if it doesn't exist
             and just update if data exist.
            :param dataframe: dataframe with new data
            :param condupdate: In What condition the data will be updated
            :param label_origem: alias name for the dataframe
            :param label_destino: alias name for the deltatable
            :return: None
        """

        self._deltatable.alias(label_destino) \
            .merge(source=dataframe.alias(label_origem),
                   condition=condupdate) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    @log.logs
    def upsert_deltatable_with_delete(self, dataframe: DataFrame, label_origem: str,
                                      label_destino: str, condupdate: str, cond_delete: str) -> None:
        """
            Upsert is a operation that insert a data if it doesn't exist
            and just update if data exist.
            you can delete basede on a condition.
            exemple code:            §
            :param dataframe: dataframe with new data
            :param label_origem: label usada to write a query
            :param label_destino: label usada to write a query
            :param condupdate: In What condition the data will be updated or inserted
            :param cond_delete: In What condition the data will be deleted
            :return: None
        """

        self._deltatable.alias(label_destino) \
            .merge(source=dataframe.alias(label_origem),
                   condition=condupdate) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .whenMatchedDelete(condition=cond_delete) \
            .execute()

    @log.logs
    def upsert_table(self, deltatable: DeltaTable,
                     label_origem: str,
                     label_destino: str,
                     condupdate: str,
                     match_fields: dict) -> None:
        """

        :param deltatable: the Deltatable with new data
        :param label_origem: alias for the deltatable dataframe
        :param label_destino: alias for the Deltatable receiving the info
        :param condupdate: In What condition the data will be updated or inserted
        :param match_fields: what fields to change
        :return:
        """

        self._deltatable.alias(label_destino) \
            .merge(source=deltatable.toDF().alias(label_origem),
                   condition=condupdate) \
            .whenMatchedUpdate(set=match_fields) \
            .execute()

    @log.logs
    def upsert_from_df(self, dataframe,
                       label_origem: str,
                       label_destino: str,
                       condupdate: str,
                       match_fields: dict) -> None:
        """

        :param dataframe: dataframe with new data
        :param label_origem: alias for the deltatable dataframe
        :param label_destino: alias for the Deltatable receiving the info
        :param condupdate: In What condition the data will be updated or inserted
        :param match_fields: what fields to change
        :return:
        """

        self._deltatable.alias(label_destino) \
            .merge(source=dataframe.alias(label_origem),
                   condition=condupdate) \
            .whenMatchedUpdate(set=match_fields) \
            .execute()
