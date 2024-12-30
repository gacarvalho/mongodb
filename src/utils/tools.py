import os
import logging
import json
import pymongo
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, max as spark_max, upper, udf
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, LongType,
    IntegerType, BooleanType, FloatType, TimestampType, DateType, MapType, BinaryType
)
from datetime import datetime
from pathlib import Path
from unidecode import unidecode
from urllib.parse import quote_plus
import subprocess


# Configuração de logging global
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------------------------------- Enviroments conexão com MongoDB ----------------------------------------------------------
mongo_user = os.environ["MONGO_USER"]
mongo_pass = os.environ["MONGO_PASS"]
mongo_host = os.environ["MONGO_HOST"]
mongo_port = os.environ["MONGO_PORT"]
mongo_db = os.environ["MONGO_DB"]

        
# ---------------------------------------------- Escapar nome de usuário e senha ----------------------------------------------
# A função quote_plus transforma caracteres especiais em seu equivalente escapado, de modo que o
# URI seja aceito pelo MongoDB. Por exemplo, m@ngo será convertido para m%40ngo.
escaped_user = quote_plus(mongo_user)
escaped_pass = quote_plus(mongo_pass)

# Função para remover acentos com UDF
def remove_accents(text: str) -> str:
    
    if text is None:
        return None 
    
    return unidecode(text)

remove_accents_udf = F.udf(remove_accents, StringType())

def load_mongo_config():

    # Função para carregar as credenciais de MongoDB de um arquivo externo
    return {
        "mongo_user": escaped_user,
        "mongo_pass": escaped_pass,
        "mongo_host": mongo_host,
        "mongo_port": mongo_port,
        "mongo_db": mongo_db
    }


def read_data_mongo(spark: SparkSession, pathSource: str, table_name: str) -> DataFrame:
    """
    Lê dados de uma tabela no MongoDB e, se houver, verifica o último timestamp disponível
    em um diretório HDFS para continuar o processamento incremental.
    """

    try:
        # Verifica se o caminho no HDFS existe
        if not hdfs_path_exists(pathSource):
            logging.info(f"[*] Diretório {pathSource} não existe. Fazendo leitura completa do MongoDB.")
            df = spark.read \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("collection", table_name) \
                .load()
        else:
            # Leitura incremental baseada no último timestamp
            df_existing = spark.read.parquet(pathSource)
            last_partition_date = df_existing.select(spark_max("odate")).collect()[0][0]
            last_partition_date_iso = datetime.strptime(last_partition_date, "%Y%m%d").isoformat()

            logging.info(f"[*] Lendo dados a partir de timestamp >= {last_partition_date_iso}")
            df = spark.read \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("collection", table_name) \
                .load() \
                .filter(col("timestamp") >= last_partition_date_iso)

    except Exception as e:

        logging.info(f"[*] Particionamento inexistente, realizando carga full!")
        logging.info("[*] Fazendo leitura completa do MongoDB.")

        df = spark.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("collection", table_name) \
            .load()

    return df

def hdfs_path_exists(path: str) -> bool:
    """
    Verifica se o caminho existe no HDFS.
    """
    try:
        result = subprocess.run(["hdfs", "dfs", "-test", "-e", path], check=False)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        logging.error(f"[*] Erro ao verificar o caminho no HDFS: {e}")
        return False

def process_reviews(df: DataFrame, table_name: str) -> DataFrame:
    """
    Aplica transformações no DataFrame de reviews, removendo acentos e formatando os dados.
    """
    logging.info(f"[*] Processando o tratamento da camada histórica para {table_name}")

    df_transformed = df.select(
        F.col("_id.oid").alias("id"),
        F.upper(remove_accents_udf(F.col("comment"))).alias("comment"),
        "votes_count",
        F.upper(remove_accents_udf(F.col("os"))).alias("os"),
        "os_version",
        F.upper(remove_accents_udf(F.col("country"))).alias("country"),
        "age",
        "customer_id",
        "cpf",
        "app_version",
        "rating",
        "timestamp"
    ).withColumn("app", lit(table_name))

    return df_transformed

def save_reviews(reviews_df: DataFrame, directory: str):
    """
    Salva o DataFrame no formato parquet, criando o diretório se necessário.
    """
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
        reviews_df.write.option("compression", "snappy").mode("overwrite").parquet(directory)
        logging.info(f"[*] Dados salvos em {directory} no formato parquet.")

    except Exception as e:
        logging.error(f"[*] Erro ao salvar os dados: {e}")
        raise

def get_schema(df, schema):
    """
    Obtém o DataFrame a seguir o schema especificado.
    """
    for field in schema.fields:
        if field.dataType == IntegerType():
            df = df.withColumn(field.name, df[field.name].cast(IntegerType()))
        elif field.dataType == StringType():
            df = df.withColumn(field.name, df[field.name].cast(StringType()))
    return df.select([field.name for field in schema.fields])


def write_to_mongo(spark: SparkSession, feedback_data: dict, collection_name: str):
    """
    Escreve dados no MongoDB a partir de um dicionário ou lista de dicionários.
    """
    try:
        # Conexão com o MongoDB
        mongo_uri = f"mongodb://{escaped_user}:{escaped_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource={mongo_db}&maxPoolSize=1"
        client = pymongo.MongoClient(mongo_uri)

        db = client[mongo_db]
        collection = db[collection_name]

        # Inserção de dados
        if isinstance(feedback_data, dict):
            collection.insert_one(feedback_data)
        elif isinstance(feedback_data, list):
            collection.insert_many(feedback_data)
        else:
            logging.error("[*] Os dados devem ser um dicionário ou uma lista de dicionários.")
            raise ValueError("[*] Formato de dados inválido para inserção no MongoDB.")

        logging.info(f"[*] Dados inseridos com sucesso na coleção {collection_name}.")

    except Exception as e:
        logging.error(f"[*] Erro ao escrever no MongoDB: {e}")
        raise

    finally:
        client.close()
        spark.stop()

def save_metrics_job_fail(metrics_json):
    """
    Salva as métricas no MongoDB.
    """
    try:
        metrics_data = json.loads(metrics_json)
        write_to_mongo(metrics_data, "dt_datametrics_fail_compass")
        logging.info(f"[*] Métricas da aplicação salvas: {metrics_json}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)
