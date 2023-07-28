import pyspark.sql.functions as F
from tablehandler import TableHandler
import pyspark.sql.dataframe as dataframetype
from util import log
from variables import aws_data
from moves3function import prepare_moving_folder


class DeltaProcessing:
    def __init__(
            self,
            environment_data,
            spark,
            **kwargs

    ):
        """
        Classe para instanciar a execucao da bronze e silver
        :param environment_data: dicionario com as bases
        :param spark: sessao spark
        :param kwargs: kwargs
        """

        self.spark = spark
        self.environment_data = environment_data
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.spark.sql("set spark.sql.parquet.int96RebaseModeInRead=LEGACY")
        self.spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=LEGACY")
        self.spark.sql("set spark.sql.parquet.int96RebaseModeInWrite=LEGACY")
        self.spark.sql("set spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY")
        self.spark.sql("set spark.sql.parquet.datetimeRebaseModeInRead=LEGACY")
        self.spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY")
        self.param = {}
        self.kwargs_param(**kwargs)
        self.tablehandler_bronze = TableHandler(self.spark)
        self.tablehandler_silver = TableHandler(self.spark)
        self.keys = []

    def kwargs_param(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        self.param = {
            'header': 'true',
            'inferSchema': 'true',
            'format_out': 'delta',
            'mode': 'overwrite',
            'format_in': 'parquet',
            'upsert': True,
            'upsert_delete': False,
        }
        self.param.update(kwargs)
        self.keys = list(self.param.keys())

    @log.logs
    def run_query(self, df: dataframetype, prefix: str, operation: dict, sql_query: str):
        """
        Executa a query sql e prepara o dataframe pra bronze/silver
        :param df: dataframe from the method
        :param prefix: the table name
        :param operation: dictionary with the tables
        :param sql_query: the sql query to be executed
        :return: dataframe after the query with 2 more fields
        """
        # criando a view do dataframe
        df.createOrReplaceTempView(operation[prefix]["table_tmp"])
        # executando a operacao sql do dataframe
        df = self.spark.sql(operation[prefix][sql_query])
        # inserindo as colunas processed com False e creationDate com a data de hoje
        df = df.withColumn(operation[prefix]["insert_fields"][0], F.lit(False)) \
            .withColumn(operation[prefix]["insert_fields"][1], F.lit(F.current_timestamp()))
        # removendo os duplicados com base no _id criado na sql query
        df = df.dropDuplicates(subset=[operation[prefix]["primary_key"]])
        self.spark.catalog.dropTempView(operation[prefix]["table_tmp"])
        return df


class DeltaProcessingBronze(DeltaProcessing):
    def run_bronze(self, table_name: str, operation: dict, sql_query: str, **kwargs):
        """
        Executa o processamento da bronze de uma tabela
        :param table_name: table name
        :param operation: dictionary with the tables
        :param sql_query: the query the method run_query will execute
        :return:
        """
        if not operation[table_name].get(sql_query):
            return
        self.kwargs_param(**kwargs)
        fmsg = f'{DeltaProcessingBronze.__name__}.{self.run_bronze.__name__}'
        logger = log.createLogger(fmsg)
        logger.info(f"iniciando a bronze da {table_name}")
        # declarando os locais da landingzone e da bronze
        pathlandzone = f"{self.environment_data['landing_zone']}/{table_name}/"
        pathbronze = f"{self.environment_data['bronze']}/{table_name}/"
        readlandzone = TableHandler(self.spark)
        # realizando a leitura da landing zone
        try:
            dataframe = readlandzone.get_table(pathlandzone, self.param)
        except Exception as err:
            logger.warning(f"landing zone para a base {table_name} vazia ou irregular, base ignorada")
            logger.warning(err)
            return
        # executando o metodo para executar a query sql da bronze
        try:
            dataframe = self.run_query(dataframe, table_name, operation, sql_query)
        except Exception as err:
            logger.error(f"erro na execucao da query sql para bronze da base {table_name}: {err}")
            logger.warning(f"devido ao erro da {table_name}, sera ignorada")
            return

        # criando a bronze se ela nao existir, ou realizando o upsert caso ja exista
        self.tablehandler_bronze.set_deltatable_path(pathbronze)
        if not self.tablehandler_bronze.is_deltatable():
            self.tablehandler_bronze.write_table(dataframe, pathbronze, self.param)
            self.spark.sql(f"""
                ALTER TABLE delta.`{pathbronze}` SET TBLPROPERTIES(
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5',
                'mergeSchema' = 'true',
                'changeDataFeed' = 'true')
            """)
        else:
            self.tablehandler_bronze.upsert_deltatable(dataframe, operation[table_name]["label_orig"],
                                                       operation[table_name]["label_destino"],
                                                       operation[table_name]["condition"])
        bucket = aws_data["landing_zone"].split("//")[-1]
        prepare_moving_folder(bucket, f'{table_name}/')

        # coloca o dataframe disponivel para remocao caso spark precise de memoria
        dataframe.unpersist()

        logger.info(f"bronze da {table_name} concluida")


class DeltaProcessingSilver(DeltaProcessing):
    def run_silver(self, table_name: str, operation: dict, sql_query: str, **kwargs):
        """
        Executa o processamento da silver de uma tabela
        :param table_name: table name
        :param operation: dictionary with the tables
        :param sql_query: the query the method run_query will execute
        :return:
        """
        if not operation[table_name].get(sql_query):
            return
        self.kwargs_param(**kwargs)
        fmsg = f'{DeltaProcessingSilver.__name__}.{self.run_silver.__name__}'
        logger = log.createLogger(fmsg)
        logger.info(f"iniciando a silver da {table_name}")
        # declarando os locais da bronze e silver
        pathsilver = f"{self.environment_data['silver']}/{table_name}/"
        pathbronze = f"{self.environment_data['bronze']}/{table_name}/"
        # checando se a bronze ja existe
        self.tablehandler_bronze.set_deltatable_path(pathbronze)
        if not self.tablehandler_bronze.is_deltatable():
            logger.error(f"bronze para a base {table_name} nao existe")
            logger.warning(f"base ignorada")
            return
        # transformando a bronze em um dataframe spark
        dataframe_bronze = self.tablehandler_bronze.get_deltatable().toDF()
        # executando o metodo para executar a query sql da silver
        try:
            dataframe = self.run_query(dataframe_bronze, table_name, operation, sql_query)
        except Exception as err:
            logger.error(f"erro na execucao da query sql para silver da base {table_name}: {err}")
            logger.warning(f"devido ao erro da {table_name}, sera ignorada")
            dataframe_bronze.unpersist()
            return
        # checando se o dataframe esta vazio
        if dataframe.isEmpty():
            logger.warning(f"nao ha dados da bronze da base {table_name} para processamento, sera ignorada")
            dataframe_bronze.unpersist()
            dataframe.unpersist()
            return
        # criando a silver se ela nao existir, ou realizando o upsert caso ja exista
        self.tablehandler_silver.set_deltatable_path(pathsilver)
        if not self.tablehandler_silver.is_deltatable():
            self.tablehandler_silver.write_table(dataframe, pathsilver, self.param)
            self.spark.sql(f"""
                ALTER TABLE delta.`{pathsilver}` SET TBLPROPERTIES(
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5',
                'mergeSchema' = 'true',
                'changeDataFeed' = 'true')
            """)
        else:
            self.tablehandler_silver.upsert_deltatable(dataframe, operation[table_name]["label_orig"],
                                                       operation[table_name]["label_destino"],
                                                       operation[table_name]["condition"])
        # checando se a silver existe, e realizando o upsert na bronze para alterar o processed
        if self.tablehandler_silver.is_deltatable():
            self.tablehandler_bronze.upsert_from_df(dataframe,
                                                    operation[table_name]["label_orig"],
                                                    operation[table_name]["label_destino"],
                                                    operation[table_name]["condition"],
                                                    operation[table_name]["match_filds"])

        # coloca o dataframe disponivel para remocao caso spark precise de memoria
        dataframe_bronze.unpersist()
        dataframe.unpersist()

        logger.info(f"silver da {table_name} concluida")

    def run_silver_nok(self, table_name: str, operation: dict, sql_query: str, **kwargs):
        """
        Executa o processamento da silver nao ok de uma tabela
        :param table_name: table name
        :param operation: dictionary with the tables
        :param sql_query: the query the method run_query will execute
        :return:
        """
        if not operation[table_name].get(sql_query):
            return
        self.kwargs_param(**kwargs)
        fmsg = f'{DeltaProcessingSilver.__name__}.{self.run_silver_nok.__name__}'
        logger = log.createLogger(fmsg)
        logger.info(f"iniciando a silver_nok da {table_name}")
        # declarando os locais da bronze e silver nao ok
        pathsilver_nok = f"{self.environment_data['silver']}/{table_name}_nOK/"
        pathbronze = f"{self.environment_data['bronze']}/{table_name}/"
        # checando se a bronze ja existe
        self.tablehandler_bronze.set_deltatable_path(pathbronze)
        if not self.tablehandler_bronze.is_deltatable():
            logger.error(f"bronze para a base {table_name} nao existe")
            logger.warning(f"base ignorada")
            return
        # transformando a bronze em um dataframe spark
        dataframe_bronze = self.tablehandler_bronze.get_deltatable().toDF()
        # executando o metodo para executar a query sql da silver nao ok
        try:
            dataframe = self.run_query(dataframe_bronze, table_name, operation, sql_query)
        except Exception as err:
            logger.error(f"erro na execucao da query sql para silve da base {table_name}: {err}")
            logger.warning(f"devido ao erro da {table_name}, sera ignorada")
            dataframe_bronze.unpersist()
            return
        # checando se o dataframe esta vazio
        if dataframe.isEmpty():
            logger.warning(f"nao ha dados da bronze da base {table_name} para processamento, sera ignorada")
            dataframe_bronze.unpersist()
            dataframe.unpersist()
            return
        # criando a silver nao ok se ela nao existir, ou realizando o upsert caso ja exista
        self.tablehandler_silver.set_deltatable_path(pathsilver_nok)
        if not self.tablehandler_silver.is_deltatable():
            self.tablehandler_silver.write_table(dataframe, pathsilver_nok, self.param)
            self.spark.sql(f"""
                ALTER TABLE delta.`{pathsilver_nok}` SET TBLPROPERTIES(
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5',
                'mergeSchema' = 'true',
                'changeDataFeed' = 'true')
            """)
        else:
            self.tablehandler_silver.upsert_deltatable(dataframe, operation[table_name]["label_orig"],
                                                       operation[table_name]["label_destino"],
                                                       operation[table_name]["condition"])
        # checando se a silver nao ok existe, e realizando o upsert na bronze para alterar o processed
        if self.tablehandler_silver.is_deltatable():
            self.tablehandler_bronze.upsert_from_df(dataframe,
                                                    operation[table_name]["label_orig"],
                                                    operation[table_name]["label_destino"],
                                                    operation[table_name]["condition"],
                                                    operation[table_name]["match_filds"])

        # coloca o dataframe disponivel para remocao caso spark precise de memoria
        dataframe_bronze.unpersist()
        dataframe.unpersist()

        logger.info(f"silver_nok da {table_name} concluida")
