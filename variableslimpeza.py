from cloud.basic_s3_functions import read_json_from_s3_object
config = read_json_from_s3_object("datalake-config-933447815926", "SPARK/pjus_spark.json")
limpeza = read_json_from_s3_object("datalake-config-933447815926", "LIMPEZA/config_limpeza.json")
tables_config_dict = config['tables_config_dict']
upsert = True
movefolder = config['movefolder']
aws_data = config['aws_data']
transform_data = config['transform_data']
gold_operations = config['gold_operations']
