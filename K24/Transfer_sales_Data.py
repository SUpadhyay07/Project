from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create SparkSession
spark = SparkSession.builder \
    .appName("TransferSalesData") \
    .config('spark.jars','./postgresql-42.7.3.jar')\
    .config('spark.driver.extraClassPath','./postgresql-42.7.3.jar')\
    .getOrCreate()
print("Defining schema")
# Define the schema for the CSV data
schema = StructType([
    StructField("TransactionID", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("SalePrice", FloatType(), True),
    StructField("PurchasePrice", FloatType(), True)
])
print("Reading cleaned csv")
# Read the CSV data into a Spark DataFrame
sales_data = spark.read.csv("/home/jovyan/work/sales_data_cleaned.csv", schema=schema, header=True)

print("Printing Schema of  cleaned csv")

sales_data.printSchema()

print("shwoing data from sales_data_cleaned csv ")
sales_data.show()


PSQL_SERVERNAME = "host.docker.internal"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "K24_DB"
PSQL_USERNAME = "test"
PSQL_PASSWORD = "Master123!"

table_sales = "sales_data"

URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"


#writing Data to postgresql Table
print("writing Data to postgresql Table")
sales_data.write.format("jdbc")\
    .option("url",URL)\
    .option("dbtable",table_sales)\
    .option("user",PSQL_USERNAME)\
    .option("password",PSQL_PASSWORD)\
    .mode("overwrite")\
    .save()




# Stop the SparkSession
#spark.stop()
