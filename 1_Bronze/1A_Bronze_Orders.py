# Databricks notebook source
# MAGIC %md ## Importação de Bibliotecas

# COMMAND ----------

import pyspark

# COMMAND ----------

# MAGIC %md ## Métodos Úteis

# COMMAND ----------

def import_files(list_files):
    data_frame = None
    try:
        data_frame = (spark.read
                      .option('header', 'true')
                      .option('inferSchema', 'false')
                      .option('delimiter', ',')
                      .option('multiline', 'true')
                      .option('escape', '\"')
                      .csv(list_files))
    except pyspark.sql.utils.AnalysisException as E:
        print('Arquivo Inexistente')
    finally:
        return data_frame
    
def find_files(list_files, word):
    result = filter(lambda x: True if word in x.name else False, list_files)
    result = map(lambda x: x.path, result)
    return list(result)

# COMMAND ----------

# MAGIC %md ## Predefinições

# COMMAND ----------

BASE_FILE_PATH = '/FileStore/etl_pizzas/tb_fato/'
BASE_DB_PATH = '/FileStore/etl_pizzas/database/'

# COMMAND ----------

files = dbutils.fs.ls(BASE_FILE_PATH)

# COMMAND ----------

# MAGIC %md ## Processamento

# COMMAND ----------

# MAGIC %md #### Leitura de arquivos

# COMMAND ----------

files_orders = find_files(files, 'orders')
files_order_details = find_files(files, 'order_details')

# COMMAND ----------

df_orders = import_files(files_orders)
df_order_details = import_files(files_order_details)

# COMMAND ----------

# MAGIC %md ## Criação de Arquivos Parquet

# COMMAND ----------

try:
    df_order_details.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/bronze/tb_bronze_order_details')
except Exception as e:
    print(e)
try:
    df_orders.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/bronze/tb_bronze_orders')
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md ## Criação de Tabelas SQL

# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_bronze_order_details USING delta LOCATION "{BASE_DB_PATH}/bronze/tb_bronze_order_details"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_bronze_orders USING delta LOCATION "{BASE_DB_PATH}/bronze/tb_bronze_orders"')