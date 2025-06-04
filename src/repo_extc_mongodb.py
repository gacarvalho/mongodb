import os
import sys
import json
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format
from datetime import datetime
from metrics import MetricsCollector, validate_ingest
from tools import load_mongo_config, read_data_mongo, process_reviews, write_to_mongo, get_schema, save_dataframe, save_metrics
from schema_mongodb import mongodb_schema_bronze
from elasticsearch import Elasticsearch


# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constantes da Aplicação ---
PATH_TARGET_BASE = "/santander/bronze/compass/reviews/mongodb/"
ENV_PRE_VALUE = "pre"
ELASTIC_INDEX_SUCCESS = "compass_dt_datametrics"
ELASTIC_INDEX_FAIL = "compass_dt_datametrics_fail"

def main():

    # Criação da sessão Spark
    mongo_config = load_mongo_config()  # Carregar as credenciais de MongoDB
    spark = create_spark_session(mongo_config)

    try:
        # Capturar argumentos da linha de comando
        if len(sys.argv) != 5:
            logging.error("[*] Uso: spark-submit app.py <env> <nome_da_colecao> <tipo_cliente(pf_pj) ")
            sys.exit(1)

        # Entrada e captura de variaveis e parametros
        env = sys.argv[1]
        table_name = sys.argv[2]
        type_client = sys.argv[4]

        # Define o caminho do HDFS com base na data atual
        path_source = f"{PATH_TARGET_BASE}{table_name}_{type_client}/"

        # Iniciar coleta de métricas
        metrics_collector = MetricsCollector(spark)
        metrics_collector.start_collection()

        # Leitura de dados do MongoDB
        df = read_data_mongo(spark, path_source, table_name)

        if env == ENV_PRE_VALUE: # Usando constante
            df.take(10)

        # Processamento dos dados
        df_processado = process_reviews(df, table_name)

        # Validação e separação dos dados válidos e inválidos
        valid_df, invalid_df, validation_results = validate_ingest(df_processado)

        # Salvar dados válidos
        save_dataframe(
            df=valid_df,
            path=path_source,
            label="valido",
            schema=mongodb_schema_bronze(),
            partition_column="odate", # data de carga referencia
            compression="snappy"
        )

        # Coleta de métricas após o processamento
        metrics_collector.end_collection()
        metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, table_name, type_client)

        # Salvar métricas no MongoDB
        save_metrics(
            metrics_type='success',
            index=ELASTIC_INDEX_SUCCESS,
            metrics_data= metrics_json
        )

    except Exception as e:
        logging.error(f"[*] Um erro ocorreu: {e}", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e,
            client="UNKNOWN_CLIENT"
        )
    finally:
        spark.stop()

def create_spark_session(mongo_config):
    """
    Cria e retorna uma SparkSession configurada com MongoDB.
    """
    try:
        mongo_uri = f"mongodb://{mongo_config['mongo_user']}:{mongo_config['mongo_pass']}@" \
                    f"{mongo_config['mongo_host']}:{mongo_config['mongo_port']}/{mongo_config['mongo_db']}?authSource={mongo_config['mongo_db']}"

        # Configurar SparkSession com integração ao MongoDB
        return SparkSession.builder \
                .appName("App Reviews origem [MongoDB]") \
            .config("spark.mongodb.input.uri", mongo_uri) \
            .config("spark.mongodb.output.uri", mongo_uri) \
            .config("spark.sql.files.ignoreMissingFiles", "true") \
            .getOrCreate()

    except Exception as e:
        logging.error(f"[*] Falha ao criar SparkSession: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
