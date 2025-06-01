import os
import logging
import json
import pymongo
import subprocess
import pyspark.sql.functions as F
import traceback
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, max as spark_max, upper, udf, date_format, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, LongType,
    IntegerType, BooleanType, FloatType, TimestampType, DateType, MapType, BinaryType
)
from datetime import datetime
from pathlib import Path
from unidecode import unidecode
from urllib.parse import quote_plus
from elasticsearch import Elasticsearch
from typing import Optional, Union


# Configuração de logging global
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_env_var(name: str, default=None, required=False):
    """
    Busca variável de ambiente.
    Se required=True e variável não existe, lança erro.
    Se required=False, retorna default se não existir.
    """
    val = os.environ.get(name, default)
    if val is None and required:
        logging.error(f"Variável de ambiente obrigatória '{name}' não encontrada.")
        raise KeyError(f"Variável de ambiente obrigatória '{name}' não encontrada.")
    return val

def get_mongo_env_vars():
    """
    Retorna variáveis de ambiente MongoDB, com usuário e senha escapados.
    """
    mongo_user = get_env_var("MONGO_USER", "", required=True)
    mongo_pass = get_env_var("MONGO_PASS", "", required=True)
    mongo_host = get_env_var("MONGO_HOST", required=True)
    mongo_port = get_env_var("MONGO_PORT", required=True)
    mongo_db = get_env_var("MONGO_DB", required=True)

    escaped_user = quote_plus(mongo_user)
    escaped_pass = quote_plus(mongo_pass)

    return mongo_user, mongo_pass, mongo_host, mongo_port, mongo_db, escaped_user, escaped_pass


def load_mongo_config():

    # Chama a função para recuperar todas as variáveis e os valores escapados
    mongo_user, mongo_pass, mongo_host, mongo_port, mongo_db, escaped_user, escaped_pass = get_mongo_env_vars()


    # Função para carregar as credenciais de MongoDB de um arquivo externo
    return {
        "mongo_user": escaped_user,
        "mongo_pass": escaped_pass,
        "mongo_host": mongo_host,
        "mongo_port": mongo_port,
        "mongo_db": mongo_db
    }

# Função para remover acentos com UDF
def remove_accents(text: str) -> str:
    if text is None:
        return None
    return unidecode(text)

remove_accents_udf = F.udf(remove_accents, StringType())

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

def save_dataframe(df: DataFrame,
                   path: str,
                   label: str,
                   schema: Optional[dict] = None,
                   partition_column: str = "odate",
                   compression: str = "snappy") -> bool:
    """
    Salva um DataFrame Spark no formato Parquet de forma robusta e profissional.

    Parâmetros:
        df: DataFrame Spark a ser salvo
        path: Caminho de destino para salvar os dados
        label: Identificação do tipo de dados para logs (ex: 'valido', 'invalido')
        schema: Schema opcional para validação dos dados
        partition_column: Nome da coluna de partição (default: 'odate')
        compression: Tipo de compressão (default: 'snappy')

    Retorno:
        bool: True se salvou com sucesso, False caso contrário

    Exceções:
        ValueError: Se os parâmetros obrigatórios forem inválidos
        IOError: Se houver problemas ao escrever no filesystem
    """

    # Validação dos parâmetros
    if not isinstance(df, DataFrame):
        logging.error(f"Objeto passado não é um DataFrame Spark: {type(df)}")
        return False

    if not path:
        logging.error("Caminho de destino não pode ser vazio")
        return False

    # Configuração
    current_date = datetime.now().strftime('%Y%m%d')
    full_path = Path(path)

    try:
        # Aplicar schema se fornecido
        if schema:
            logging.info(f"[*] Aplicando schema para dados {label}")
            df = get_schema(df, schema)


        # Adicionar coluna de partição
        df_partition = df.withColumn(partition_column, lit(current_date))

        # Verificar se há dados
        if not df_partition.head(1):
            logging.error(f"[*] Nenhum dado {label} encontrado para salvar")
            return False

        # Preparar diretório
        try:
            full_path.mkdir(parents=True, exist_ok=True)
            logging.debug(f"[*] Diretório {full_path} verificado/criado")

        except Exception as dir_error:
            logging.error(f"[*] Falha ao preparar diretório {full_path}: {dir_error}")
            raise IOError(f"[*] Erro de diretório: {dir_error}") from dir_error

        # Escrever dados
        logging.info(f"[*] Salvando {df_partition.count()} registros ({label}) em {full_path}")

        (df_partition.write
         .option("compression", compression)
         .mode("overwrite")
         .partitionBy(partition_column)
         .parquet(str(full_path)))

        logging.info(f"[*] Dados {label} salvos com sucesso em {full_path}")
        return True

    except Exception as e:
        error_msg = f"[*] Falha ao salvar dados {label} em {full_path}"
        logging.error(error_msg, exc_info=True)
        logging.error(f"[*] Detalhes do erro: {str(e)}\n{traceback.format_exc()}")
        return False

def save_metrics(metrics_type: str,
                 index: str,
                 error: Optional[Exception] = None,
                 client: Optional[str] = None,
                 metrics_data: Optional[Union[dict, str]] = None) -> None:
    """
    Salva métricas no Elasticsearch mantendo estruturas específicas para cada tipo.

    Parâmetros:
        metrics_type: 'success' ou 'fail' (case insensitive)
        index: Nome do índice no Elasticsearch
        error: Objeto de exceção (obrigatório para tipo 'fail')
        client: Nome do cliente (opcional)
        metrics_data: Dados das métricas (obrigatório para 'success', ignorado para 'fail')

    Estruturas:
        - FAIL: Mantém estrutura fixa com informações de erro
        - SUCCESS: Mantém exatamente o que foi passado em metrics_data
    """
    # Converter para minúsculas para padronização
    metrics_type = metrics_type.lower()

    # Validações iniciais
    if metrics_type not in ('success', 'fail'):
        raise ValueError("[*] O tipo deve ser 'success' ou 'fail'")

    if metrics_type == 'fail' and not error:
        raise ValueError("[*] Para tipo 'fail', o parâmetro 'error' é obrigatório")

    if metrics_type == 'success' and not metrics_data:
        raise ValueError("[*] Para tipo 'success', 'metrics_data' é obrigatório")

    # Configuração do Elasticsearch
    ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
    ES_USER = os.getenv("ES_USER")
    ES_PASS = os.getenv("ES_PASS")

    if not all([ES_USER, ES_PASS]):
        raise ValueError("[*] Credenciais do Elasticsearch não configuradas")

    # Preparar o documento conforme o tipo
    if metrics_type == 'fail':
        document = {
            "timestamp": date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            "layer": "bronze",
            "project": "compass",
            "job": "mongodb_reviews",
            "priority": "0",
            "torre": "SBBR_COMPASS",
            "client": client.upper() if client else "UNKNOWN_CLIENT",
            "error": str(error)
        }
    else:
        # Para success, usar exatamente o metrics_data fornecido
        if isinstance(metrics_data, str):
            try:
                document = json.loads(metrics_data)
            except json.JSONDecodeError as e:
                raise ValueError("[*] metrics_data não é um JSON válido") from e
        else:
            document = metrics_data

    # Conexão e envio para Elasticsearch
    try:
        es = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASS),
            request_timeout=30
        )

        response = es.index(
            index=index,
            document=document
        )

        logging.info(f"[*] Métricas salvas com sucesso no índice {index}. ID: {response['_id']}")
        return response

    except ElasticsearchException as es_error:
        logging.error(f"[*] Falha ao salvar no Elasticsearch: {str(es_error)}")
        raise
    except Exception as e:
        logging.error(f"[*] Erro inesperado: {str(e)}")
        raise