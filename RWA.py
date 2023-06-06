# Databricks notebook source
# MAGIC %md
# MAGIC #Take Home Assignment

# COMMAND ----------

# MAGIC %md
# MAGIC ###Installing libraries

# COMMAND ----------

# MAGIC %pip install eth_abi pycryptodome

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load Ethereum Logs And Tokens

# COMMAND ----------

from pyspark.sql.functions import expr

# Specify the S3 bucket path
s3_bucket_path = "s3a://rwa-xyz-recruiting-data/eth_logs_decoded/"

# Load the data into an RDD
df = spark.read.parquet(s3_bucket_path)
df.repartition(expr("weekofyear(block_date)"))
df.cache()

# create temp view to directly interact with pyspark sql 
df.createTempView("ethereum_logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Generate topic name for 'Transfer' event 

# COMMAND ----------

from Crypto.Hash import keccak
k = keccak.new(digest_bits=256)
k.update(b'Transfer(address,address,uint256)')
topic = '0x' + k.hexdigest()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Generate results for most transferred ERC-20 token per week in 2023

# COMMAND ----------

def generate(sql_statement, file_path):
    df = spark.sql(sql_statement) 
    df.toPandas().to_csv(file_path, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Based on sum of transfer amounts

# COMMAND ----------

sql = """
        WITH transfer AS (
            SELECT 
                month(e.block_date) as month_of_year,
                weekofyear(e.block_date) as week_of_year, 
                t.address, 
                sum(cast(get_json_object(params, '$.value') as decimal(38,2))) as transfer_metric 
            FROM ethereum_logs e
            JOIN tokens t ON t.address = e.address
            WHERE e.topic0 = '{}' 
            AND year(e.block_date) = '2023' 
            AND e.params like '%\"value\"%'
            GROUP BY month(e.block_date), weekofyear(e.block_date), t.address
        ),
        ranked AS (
            SELECT 
                month_of_year, 
                address, 
                transfer_metric,
                ROW_NUMBER() OVER (PARTITION BY week_of_year ORDER BY transfer_metric DESC) AS rn
            FROM transfer
        )
        SELECT month_of_year, address, transfer_metric
        FROM ranked
        WHERE rn = 1 
        ORDER BY month_of_year
""".format(topic)

# COMMAND ----------

generate(sql, 'output/sum.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Based on count of transfer

# COMMAND ----------

sql = """
        WITH transfer AS (
            SELECT 
                month(e.block_date) as month_of_year,
                weekofyear(e.block_date) as week_of_year, 
                t.address, 
                count(1) as transfer_metric 
            FROM ethereum_logs e
            JOIN tokens t ON t.address = e.address
            WHERE e.topic0 = '{}' 
            AND year(e.block_date) = '2023' 
            AND e.params like '%\"value\"%'
            GROUP BY month(e.block_date), weekofyear(e.block_date), t.address
        ),
        ranked AS (
            SELECT 
                month_of_year, 
                address, 
                transfer_metric,
                ROW_NUMBER() OVER (PARTITION BY week_of_year ORDER BY transfer_metric DESC) AS rn
            FROM transfer
        )
        SELECT month_of_year, address, transfer_metric
        FROM ranked
        WHERE rn = 1 
        ORDER BY month_of_year
""".format(topic)

# COMMAND ----------

generate(sql, 'output/count.csv')
