from pyspark.sql import SparkSession
from pyspark.sql.functions import col
print("===creating spark session===")
spark = SparkSession.builder \
    .appName("ProcessSalesData") \
    .config('spark.jars', './postgresql-42.7.3.jar') \
    .config('spark.driver.extraClassPath', './postgresql-42.7.3.jar') \
    .getOrCreate()
print("===Defining Connection Parameters===")
PSQL_SERVERNAME = "host.docker.internal"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "K24_DB"
PSQL_USERNAME = "test"
PSQL_PASSWORD = "Master123!"

table_sales = "sales_data"

URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"


def process_data() -> tuple[dict[int, float], dict[int, float], list[int]]:
    # Create a SparkSession
    print("====Start reading the data from postgretable sales_data======")
    # Read data from PostgreSQL table
    sales_data = spark.read \
        .format("jdbc") \
        .option("url", URL) \
        .option("dbtable",table_sales) \
        .option("user", PSQL_USERNAME) \
        .option("password",PSQL_PASSWORD) \
        .load()
    print("===Display sales Data===")
    sales_data.show()
    
    print("===Calculate total profit for each transaction===")
    # Calculate total profit for each transaction
    sales_data = sales_data.withColumn("TotalProfit", col("Quantity") * (col("SalePrice") - col("PurchasePrice")))
    print("===Group by transaction ID and sum total profit for each transaction===")
    # Group by transaction ID and sum total profit for each transaction
    transaction_profit = sales_data.groupBy("TransactionID").sum("TotalProfit").rdd.collectAsMap()
    
    print("===Group by product ID and sum total profit for each product===")
    # Group by product ID and sum total profit for each product
    product_profit = sales_data.groupBy("ProductID").sum("TotalProfit").rdd.collectAsMap()
    print("===Identify top 2 selling products based on total quantity===")
    
    # Identify top 2 selling products based on total quantity
    top_selling_products = sales_data.groupBy("ProductID").sum("Quantity").orderBy(col("sum(Quantity)").desc()).limit(2).rdd.map(lambda x: x[0]).collect()

    # Stop SparkSession
    #spark.stop()
    print("===Round total profits to 2 decimal places===")
    # Round total profits to 2 decimal places
    transaction_profit = {int(k): round(v, 2) for k, v in transaction_profit.items()}
    product_profit = {int(k): round(v, 2) for k, v in product_profit.items()}

    return transaction_profit, product_profit, top_selling_products

# Example usage
transaction_profit, product_profit, top_selling_products = process_data()
print("===Printing Response====")
print(transaction_profit)
print(product_profit)
print(top_selling_products)