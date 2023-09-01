import spark_init
from variableslimpeza import tables_config_dict, limpeza, gold_operations, aws_data
from classes.removes3files import remove_old_files


if __name__ == "__main__":

    spark_start = spark_init.SparkInit()
    spark = spark_start.get_spark_session()

    # realiza a otimizacao e limpeza bronze e silver
    for table_name, parameters_dict in list(tables_config_dict.items()):
        print(table_name)
        spark.sql(f'OPTIMIZE delta.`{aws_data["bronze"]}/{table_name}/`')
        spark.sql(f'VACUUM delta.`{aws_data["bronze"]}/{table_name}/` RETAIN {limpeza["hours"]} HOURS')
        try:
            spark.sql(f'OPTIMIZE delta.`s3a://{aws_data["silver"]}/{table_name}/`')
            spark.sql(f'VACUUM delta.`s3a://{aws_data["silver"]}/{table_name}/` RETAIN {limpeza["hours"]} HOURS')
            print()
        except Exception as err:
            print(f"silver {table_name} nao existe: {err}\n")

    # realiza a otimizacao e limpeza gold
    for operations in gold_operations.values():
        try:
            spark.sql(f'OPTIMIZE delta.`{aws_data["gold"]}/{operations["table_name"]}/`')
            spark.sql(f'VACUUM delta.`{aws_data["gold"]}/{operations["table_name"]}/` RETAIN {limpeza["hours"]} HOURS')
        except Exception as err:
            print(f"gold {operations['table_name']} nao existe: {err}\n")
    spark.stop()

    # remove arquivos do bucket que ja foram processados e nao tem mais uso nem para auditoria
    remove_old_files(limpeza["bucket"], limpeza["hours"])
