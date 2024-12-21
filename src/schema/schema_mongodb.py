from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def mongodb_schema_bronze():
    return StructType([
    StructField('id', StringType(), True),
    StructField('comment', StringType(), True),
    StructField('votes_count', IntegerType(), True),
    StructField('os', StringType(), True),
    StructField('os_version', StringType(), True),
    StructField('country', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('customer_id', StringType(), True),
    StructField('cpf', StringType(), True),
    StructField('app_version', StringType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', StringType(), True),
    StructField('app', StringType(), True)
])
