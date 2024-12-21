import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType
from pyspark.sql import Row
from datetime import datetime
from src.utils.tools import read_data_mongo, process_reviews, get_schema
from src.metrics.metrics import validate_ingest
from src.schema.schema_mongodb import mongodb_schema_bronze


# Criando uma fixture para o SparkSession
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    yield spark
    spark.stop()

# Teste unitário para a função read_data_mongo
@patch.dict(os.environ, {
    "MONGO_USER": "test_user",
    "MONGO_PASS": "test_pass",
    "MONGO_HOST": "localhost",
    "MONGO_PORT": "27017",
    "MONGO_DB": "test_database"
})

def test_args():
    """
    Testa os argumentos passados via linha de comando para garantir que o número correto de argumentos é fornecido.
    """
    with patch.object(sys, 'argv', ['app.py', 'banco-santander-br']):
        # Verifica se o número correto de argumentos foi passado
        assert len(sys.argv) == 2, "[*] Usage: spark-submit app.py <review_id> <name_app>"

def mock_data(spark) -> DataFrame:
    # Simulando a entrada de dados de reviews que seria o DataFrame da função test_read_data_incremental_load
    schema = StructType([
        StructField("_id", StructType([StructField("oid", StringType(), True)]), True),  # Definindo o _id como um Struct com o campo oid
        StructField("customer_id", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("country", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("age", StringType(), True),
        StructField("votes_count", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("os", StringType(), True),
        StructField("os_version", StringType(), True)
    ])

    # DataFrame com os dados simulados
    return spark.createDataFrame([
        Row(_id=Row(oid="674b9ac87d0fe87b5624f626"), customer_id=8148, rating=3, comment="Gostei muito do app, muito útil!", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="895.136.472-00", age=64, votes_count=7, ip_address="172.18.93.250", os="IOS", os_version="10.0"),
        Row(_id=Row(oid="674b9ac87d0fe87b5624f627"), customer_id=8349, rating=4, comment="Cumpre o que promete, nada de mais.", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="791.086.324-13", age=63, votes_count=10, ip_address="10.220.137.17", os="Android", os_version="18.04"),
        Row(_id=Row(oid="674b9ac87d0fe87b5624f629"), customer_id=4552, rating=3, comment="Gostei muito do app, muito útil!", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="276.809.543-56", age=34, votes_count=5, ip_address="192.168.42.193", os="IOS", os_version="18.04"),
        Row(_id=Row(oid="674b9ac87d0fe87b5624f62c"), customer_id=8766, rating=3, comment="O aplicativo é bom, mas pode melhorar.", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="514.936.078-39", age=33, votes_count=6, ip_address="192.168.43.22", os="Android", os_version="10.0"),
        Row(_id=Row(oid="674b9ac87d0fe87b5624f62e"), customer_id=2934, rating=4, comment="O suporte técnico é muito ruim.", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="452.196.378-19", age=34, votes_count=3, ip_address="192.168.114.13", os="IOS", os_version="18.04"),
        Row(_id=Row(oid="674b9ac87d0fe87b5624f630"), customer_id=4124, rating=1, comment="Excelente, super recomendo!", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="983.620.175-03", age=45, votes_count=1, ip_address="172.22.118.133", os="Android", os_version="10.0"),
        Row(_id=Row(oid="674b9ac87d0fe87b5624f632"), customer_id=1145, rating=5, comment="Não gostei, tem muitos bugs.", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="601.872.435-90", age=55, votes_count=8, ip_address="192.168.255.156", os="IOS", os_version="18.04"),
        Row(_id=Row(oid="674b9ac87d0fe87b5624f632"), customer_id=1145, rating=5, comment="Não gostei, tem muitos bugs.", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="601.872.435-90", age=55, votes_count=8, ip_address="192.168.255.156", os="IOS", os_version="18.04"),
        Row(_id=Row(oid="674b9ac87d0fe87b5624f634"), customer_id=1556, rating=5, comment="A interface é confusa, difícil de usar.", timestamp="2024-11-30T23:07:37.832165", app_version="1.0.0", country="BR", cpf="410.395.762-06", age=61, votes_count=9, ip_address="10.136.231.37", os="IOS", os_version="10.0")
    ], schema)

class TestReadDataMongo:

    @patch("src.utils.tools.hdfs_path_exists")
    @patch("pyspark.sql.SparkSession.read")
    def test_read_data_full_load(self, mock_spark_read, mock_hdfs_path_exists, spark):
        # Mock para a função hdfs_path_exists retornando False (simulando que o diretório não existe)
        mock_hdfs_path_exists.return_value = False

        # Simula um DataFrame de retorno do MongoDB
        mock_df = mock_data(spark)

        # Simula o comportamento do método .load() do Spark
        mock_spark_read.format.return_value.option.return_value.load.return_value = mock_df

        # Chama a função a ser testada
        result_df = read_data_mongo(spark, "/fake/path", "banco-santander-br")

        # Verifica se a leitura retornou os dados esperados
        assert result_df.collect() == mock_df.collect()

    @patch("src.utils.tools.hdfs_path_exists")
    @patch("pyspark.sql.SparkSession.read")
    def test_read_data_incremental_load(self, mock_spark_read, mock_hdfs_path_exists, spark):
        # Mock para a função hdfs_path_exists retornando True (simulando que o diretório existe)
        mock_hdfs_path_exists.return_value = True

        # Simula o DataFrame do HDFS (últimos dados carregados)
        schema = StructType([StructField("odate", StringType(), True)])
        df_existing = spark.createDataFrame([("20241130",)], schema)

        # Simula o comportamento de leitura do HDFS
        mock_spark_read.parquet.return_value = df_existing

        mock_df_incremental = mock_data(spark)

        # Simula o comportamento do método .load() do Spark
        mock_spark_read.format.return_value.option.return_value.load.return_value = mock_df_incremental

        # Chama a função a ser testada
        result_df = read_data_mongo(spark, "/fake/path", "test_table")

        assert result_df.collect() == mock_df_incremental.collect()



class TestProcessReviews:

    @patch("src.utils.tools.process_reviews")
    def test_process_reviews(self, mock_process_reviews, spark):

        mock_df = mock_data(spark)

        # Simula o comportamento de processamento de reviews
        mock_process_reviews.return_value = "Dados Processados com sucesso!"

        # Chama a função a ser testada
        result = process_reviews(mock_df, 'banco-santander-br')

        # Verifique se o resultado corresponde ao esperado
        assert result is not None, "[*] Formato incorreto!"
        assert result.count() > 0, "[*] Sem dados validos!"

class TestValidateIngest:

    @patch("src.metrics.metrics.validate_ingest")
    def test_validate_ingest(self, mock_validate, spark):
        """
        Testa a função de coleta de avaliações mockada para garantir que o retorno não está vazio.
        """
        with patch.object(sys, 'argv', ['app.py', 'banco-santander-br']):
            table_name = sys.argv[1]

            mock_df = mock_data(spark)
            result = process_reviews(mock_df, table_name)

            # Valida o DataFrame e coleta resultados
            valid_df, invalid_df, validation_results = validate_ingest(result)

            assert valid_df.count() > 0
            assert invalid_df.count() > 0
            assert len(validation_results) > 0

class TestSchema:

    @patch("src.schema.schema_mongodb.mongodb_schema_bronze")
    # Função de teste que inclui validação do schema e dos patterns
    def test_schema(self, mock_validate, spark):
        """
        Testa a função de coleta de avaliações mockada para garantir que o retorno não está vazio e que segue o schema e os patterns esperados.
        """
        with patch.object(sys, 'argv', ['app.py', 'banco-santander-br']):
            table_name = sys.argv[1]

            mock_df = mock_data(spark)
            mock_df = mock_df.withColumn("votes_count", mock_df["votes_count"].cast(IntegerType())) \
                             .withColumn("age", mock_df["age"].cast(IntegerType())) \
                             .withColumn("rating", mock_df["rating"].cast(IntegerType()))

            result = process_reviews(mock_df, table_name)

            # Valida o DataFrame e coleta resultados
            valid_df, invalid_df, validation_results = validate_ingest(result)

            # Obtém o schema correto chamando a função mongodb_schema_bronze()
            df_schema = valid_df.schema
            expected_schema = mongodb_schema_bronze()  # Chama a função para obter o schema real

            # Agora você pode comparar df_schema com expected_schema
            expected_fields = [field.name for field in expected_schema.fields]
            df_fields = [field.name for field in df_schema.fields]

            assert sorted(expected_fields) == sorted(df_fields), f"Esperado {expected_fields}, mas obteve {df_fields}"

            # Verificando os tipos de dados para cada campo
            for expected_field, df_field in zip(expected_schema.fields, df_schema.fields):
                assert expected_field.name == df_field.name, f"Nome de campo esperado {expected_field.name}, mas obteve {df_field.name}"
                assert expected_field.dataType == df_field.dataType, f"Tipo de dado esperado {expected_field.dataType}, mas obteve {df_field.dataType}"