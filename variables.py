from cloud.basic_s3_functions import read_yaml_from_s3_object
# Alterar bucket e key do json de configuracao
deparaconfig = read_yaml_from_s3_object("test-conf-domrock", "spark_depara_conf.yaml")
kafka_config = read_yaml_from_s3_object("test-conf-domrock", "kafka_config.yaml")
