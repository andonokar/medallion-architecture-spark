from cloud.basic_s3_functions import read_yaml_from_s3_object
from classes.delta_processing import DeltaProcessingBronze, DeltaProcessingSilver
from classes.delta_gold_processing import DeltaProcessingGold
from util import log
from classes.moves3function import prepare_moving_folder


class Client:
    def __init__(self, client, setup, spark):
        self.client = client
        self.setup = setup
        self.logger = log.createLogger(client)
        self.spark = spark

    def _get_client_config(self):
        bucket = self.setup.get('bucket')
        key = self.setup.get('key')
        if not (bucket and key):
            self.logger.error('o local de configuracao esta ausente')
            return
        try:
            conf = read_yaml_from_s3_object(bucket, key)
        except Exception as err:
            self.logger.error(f'erro na leitura da configuracao: {err}')
            return
        self.conf = conf
        return True

    def _create_delta_processor(self, processor_class, aws_data, logger):
        return processor_class(aws_data, self.spark, logger)

    def _init_bronze_silver_gold(self):
        aws_data = self.conf.get('aws_data')
        if not aws_data:
            self.logger.error('a configuracao aws_data esta vazia')
            return
        self.delta_bronze = self._create_delta_processor(DeltaProcessingBronze, aws_data, self.logger)
        self.delta_silver = self._create_delta_processor(DeltaProcessingSilver, aws_data, self.logger)
        self.delta_gold = self._create_delta_processor(DeltaProcessingGold, aws_data, self.logger)
        return True

    def _sucess_bronze(self, success, table_name):
        movefolder = self.conf.get('movefolder')
        if not (success and movefolder):
            return
        url = self.conf['aws_data'].get('landing_zone').split("//")[-1]
        split = url.split('/')
        bucket = split.pop(0)
        if len(split) > 0:
            folder = f"{'/'.join(split)}/{table_name}"
        else:
            folder = table_name
        try:
            prepare_moving_folder(bucket, f'{folder}/', movefolder)
        except Exception as err:
            self.logger.error(f'erro em mover a pasta para processado: {err}')
            self.logger.warning(f'devido ao erro, a pasta nao sera movida e '
                                f'sera reprocessada novamente na proxima execucao')

    def process_delta_tables(self):
        if not self._get_client_config():
            return
        if not self._init_bronze_silver_gold():
            return
        transform_data = self.conf.get('transform_data')
        if not transform_data:
            self.logger.error('a configuracao transform_data esta vazia')
            return
        for table_name, parameters_dict in transform_data.items():
            success = self.delta_bronze.run_bronze(table_name, transform_data, "sql_create_id",
                                                   format_in=parameters_dict.get('format', 'parquet'))
            self._sucess_bronze(success, table_name)
            self.delta_silver.run_silver(table_name, transform_data, "sql_create_silver")
            self.delta_silver.run_silver_nok(table_name, transform_data, "sql_create_silver_nOK")
        gold_operations = self.conf.get('gold_operations')
        if not gold_operations:
            self.logger.warning('a configuracao gold_operations esta vazia')
            return
        for operations in gold_operations.values():
            self.delta_gold.run_gold(operations, "sql_query")
        self.logger.info("Operacao Finalizada com sucesso")
