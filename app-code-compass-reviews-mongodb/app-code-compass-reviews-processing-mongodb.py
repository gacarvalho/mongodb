import logging
import sys
import json
from pyspark.sql import SparkSession
from datetime import datetime
from metrics import MetricsCollector, validate_ingest
from tools import load_mongo_config, read_data_mongo, process_reviews, save_reviews, write_to_mongo, get_schema
from schema_mongodb import mongodb_schema_bronze

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()])
 
def main():
    try:
        # Criação da sessão Spark
        mongo_config = load_mongo_config()  # Carregar as credenciais de MongoDB
        with create_spark_session(mongo_config)as spark:

            # Capturar argumentos da linha de comando
            if len(sys.argv) != 2:
                logging.error("[*] Uso: spark-submit app.py <nome_da_colecao>")
                sys.exit(1)
            
            # Entrada e captura de variaveis e parametros
            table_name = sys.argv[1]

            # Define o caminho do HDFS com base na data atual
            date_path = datetime.now().strftime("%Y%m%d")
            path_source = f"/santander/bronze/compass/reviews/mongodb/{table_name}/odate={date_path}/"

            # Iniciar coleta de métricas
            metrics_collector = MetricsCollector(spark)
            metrics_collector.start_collection()

            # Leitura de dados do MongoDB
            df = read_data_mongo(spark, path_source, table_name)
            df.show(truncate=False)

            # Processamento dos dados
            df_processado = process_reviews(df, table_name)

            # Validação e separação dos dados válidos e inválidos
            valid_df, invalid_df, validation_results = validate_ingest(df_processado)

            # Salvar dados válidos
            save_dataframe(valid_df, path_source, "valido")

            # Salvar dados inválidos
            valid_df.show(truncate=False)


            # Coleta de métricas após o processamento
            metrics_collector.end_collection()
            metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, table_name)

            # Salvar métricas no MongoDB
            save_metrics(spark, metrics_json)

    except Exception as e:
        logging.error(f"Um erro ocorreu: {e}", exc_info=True)
        sys.exit(1)

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
            .appName("LeituraFeedback") \
            .config("spark.mongodb.input.uri", mongo_uri) \
            .config("spark.mongodb.output.uri", mongo_uri) \
            .config("spark.sql.files.ignoreMissingFiles", "true") \
            .getOrCreate()

    except Exception as e:
        logging.error(f"[*] Falha ao criar SparkSession: {e}", exc_info=True)
        raise



def save_dataframe(df, path, label):
    """
    Salva o DataFrame em formato parquet e loga a operação.
    """
    try:
        schema = mongodb_schema_bronze()
        # Alinhar o DataFrame ao schema definido
        df = get_schema(df, schema)

        if df.limit(1).count() > 0:  # Verificar existência de dados
            logging.info(f"[*] Salvando dados {label} para: {path}")
            save_reviews(df, path)
        else:
            logging.warning(f"[*] Nenhum dado {label} foi encontrado!")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar dados {label}: {e}", exc_info=True)


def save_metrics(spark, metrics_json):
    """
    Salva as métricas no MongoDB.
    """
    try:
        metrics_data = json.loads(metrics_json)
        write_to_mongo(spark, metrics_data, "dt_datametrics_compass")
        logging.info(f"Métricas da aplicação salvas: {metrics_json}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)

if __name__ == "__main__":
    main()
