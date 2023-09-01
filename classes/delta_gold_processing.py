from delta.tables import *
from classes.tablehandler import TableHandler
from util import log


class DeltaProcessingGold:
    def __init__(
            self,
            environment_data,
            spark,
            logger,
            **kwargs

    ):
        """
        Classe para instanciar a execucao da gold
        :param environment_data: dicionario com as bases
        :param spark: sessao spark
        :param kwargs: kwargs
        """
        self.df_current_table = None
        self.df_previous_table = None
        self.spark = spark
        self.environment_data = environment_data
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.param = {}
        self.keys = []
        self.kwargs_param(**kwargs)
        self.deltatables = []
        self.views = []
        self.tablehandler = TableHandler(spark)
        self.logger = logger

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
            'step': {"silver": 'silver', "gold": "gold"}
        }
        self.param.update(kwargs)
        self.keys = list(self.param.keys())

    @log.logs
    def save_update_table(self, j_df: DataFrame, operations: dict, path: str):
        """
        Cria uma gold com o dataframe, ou realiza o upsert se a gold ja existe
        :param j_df: o dataframe criado pela query sql
        :param operations: dicionario com as operacoes da gold
        :param path: o caminho para a gold ser instanciada
        """
        # instanciando a delta table
        tablehandler = TableHandler(self.spark, path)
        # checando se a deltatable ja existe
        if not tablehandler.is_deltatable():
            tablehandler.write_table(j_df, path, self.param)
            self.spark.sql(f"""
                            ALTER TABLE delta.`{path}` SET TBLPROPERTIES(
                            'delta.columnMapping.mode' = 'name',
                            'delta.minReaderVersion' = '2',
                            'delta.minWriterVersion' = '5',
                            'mergeSchema' = 'true',
                            'changeDataFeed' = 'true')
                        """)
        else:
            tablehandler.upsert_deltatable(j_df, operations["label_orig"], operations["label_destino"],
                                           operations["condition"])

    @log.logs
    def _update_silver_tables(self, operations: dict, path: str):
        """
        pega os dados da gold e realiza o upsert na silver para alterar os valores(ex: processed para True)
        :param operations: dicionario com as operacoes da gold
        :param path: o caminho para a gold ser instanciada
        """
        # checando a configuracao de upsert silver
        tables_silver = operations.get("tables_silver")
        if not tables_silver:
            raise KeyError('a chave table_silver nao esta configurada e o upsert_silver = true')
        # instanciando a delta table
        tablehandlergold = TableHandler(self.spark, path)
        # checando se a deltatable ja existe
        if tablehandlergold.is_deltatable():
            # fazendo iteracao entre as silvers
            for val in operations["join_operations"]["tables"]:
                upsert_config = tables_silver.get(val)
                if not upsert_config:
                    continue
                tablehandlersilver = TableHandler(self.spark,
                                                  f"{self.environment_data[self.param['step']['silver']]}/{val}/")

                deltatable_gold = tablehandlergold.get_deltatable()
                tablehandlersilver.upsert_table(deltatable_gold, operations["label_orig"],
                                                val, upsert_config["conditions"],
                                                operations["match_filds"])

    def run_gold(self, operations: dict, query_sql: str, **kwargs):
        """
        Executa o processamento gold de varias tabelas silver
        :param operations: dicionario com as operacoes da gold
        :param query_sql: a query a ser executada pelo spark.sql
        :param kwargs:
        :return:
        """
        self.logger.info(f"iniciando a gold da {operations['table_name']}")
        self.kwargs_param(**kwargs)
        # criando uma lista com as tabelas
        tables = operations["join_operations"]["tables"]
        # limpando a lista das deltatables
        self.deltatables.clear()
        # limpando a lista de views
        self.views.clear()
        # realizando a iteracao entre as bases silver e criando as views sql para cada silver e removendo dataframe da memoria
        for index, table in enumerate(tables):
            path = f"{self.environment_data[self.param['step']['silver']]}/{table}/"
            self.tablehandler.set_deltatable_path(path)
            if self.tablehandler.is_deltatable():
                self.deltatables.append(self.tablehandler.get_deltatable().toDF())
            else:
                self.logger.error(f'nao existe silver com path {path}')
                self.logger.warning(f"devido ao erro da {operations['table_name']}, sera ignorada")
                return
            self.views.append(self.deltatables[index].createOrReplaceTempView(tables[index]))
            self.deltatables[index].unpersist()
        # executando a query sql de cruzamentos para a gold
        try:
            df = self.spark.sql(operations[query_sql])
        except Exception as err:
            self.logger.error(f"erro na execucao da query sql para gold da base {operations['table_name']}: {err}")
            self.logger.warning(f"devido ao erro da {operations['table_name']}, sera ignorada")
            return

        # removendo duplicados caso necessario
        df = df.dropDuplicates(subset=[operations["primary_key"]])

        # checando se o dataframe da query veio vazio
        if df.isEmpty():
            self.logger.warning(f"nao ha dados da silver das bases {tables} para processamento, sera ignorada")
            return

        for name in tables:
            self.spark.catalog.dropTempView(name)
        # caminho em que a gold sera salvo
        path_to_save = f"{self.environment_data[self.param['step']['gold']]}/{operations['table_name']}/"
        if '_id' not in df.columns:
            self.logger.error(f"Erro: {operations['table_name']} não possui a chave unica _id criada")
            self.logger.warning(f"devido ao erro ocorrido, a gold {operations['table_name']} sera ignorada")
            return

        self.save_update_table(df, operations, path_to_save)

        if operations.get('upsert_silver') == 'true':
            try:
                self._update_silver_tables(operations, path_to_save)
            except Exception as err:
                self.logger.error(f"erro na atualizando as silvers que geram a tabela {operations['table_name']}: {err}")

        self.logger.info(f"gold da {operations['table_name']} concluida com sucesso")
