# Databricks notebook source
# MAGIC %md ## Importação de Bibliotecas

# COMMAND ----------

from pyspark.sql import functions as F
import datetime

# COMMAND ----------

# MAGIC %md ## Predefinições

# COMMAND ----------

BASE_FILE_PATH = '/FileStore/etl_pizzas/tb_fato/'

# COMMAND ----------

files = dbutils.fs.ls(BASE_FILE_PATH)

# COMMAND ----------

# MAGIC %md ## Processamento

# COMMAND ----------

# MAGIC %md #### Leitura de arquivos

# COMMAND ----------

df_orders = spark.table('projeto_pizza.tb_bronze_orders')
df_order_details = spark.table('projeto_pizza.tb_bronze_order_details')

# COMMAND ----------

# MAGIC %md #### Transformação de Colunas

# COMMAND ----------

df_orders = (df_orders.withColumn('order_id', F.col('order_id').cast('int').cast('string'))
                      .withColumn('date', F.to_date(F.col('date'), 'yyyy-MM-dd')))

# COMMAND ----------

# MAGIC %md #### Exclusão de Linhas Duplicadas

# COMMAND ----------

df_orders = df_orders.dropDuplicates(subset=['order_id'])

# COMMAND ----------

# MAGIC %md #### Atribuição de Valores a Campos Nulos

# COMMAND ----------

df_orders = (df_orders
             .withColumn('date', F.when(F.col('date').isNull(), datetime.date.max).otherwise(F.col('date'))).fillna('99:99:99', subset='time')
             .filter(F.col('order_id').isNotNull())
            )

# COMMAND ----------

# MAGIC %md #### Exclusão de Valores Nulos e Duplicatas 

# COMMAND ----------

df_order_details = df_order_details.dropna()
df_order_details = df_order_details.dropDuplicates(subset=['order_details_id'])


# COMMAND ----------

# MAGIC %md #### Transformação de Colunas

# COMMAND ----------

df_order_details = df_order_details.withColumn('quantity', F.col('quantity').cast('int'))

# COMMAND ----------

# MAGIC %md #### Join entre DataFrames

# COMMAND ----------

df_orders_detailed = df_orders.join(df_order_details, on=['order_id'], how='left')

# COMMAND ----------

# MAGIC %md #### Realização do Upsert

# COMMAND ----------

df_orders_detailed.createOrReplaceTempView('vw_orders_detailed')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO projeto_pizza.tb_silver_orders_detailed AS old
# MAGIC USING vw_orders_detailed AS new
# MAGIC ON old.order_details_id = new.order_details_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (
# MAGIC     old.order_id,
# MAGIC     old.date,
# MAGIC     old.time,
# MAGIC     old.order_details_id,
# MAGIC     old.pizza_id,
# MAGIC     old.quantity
# MAGIC   )
# MAGIC   VALUES(
# MAGIC     new.order_id,
# MAGIC     new.date,
# MAGIC     new.time,
# MAGIC     new.order_details_id,
# MAGIC     new.pizza_id,
# MAGIC     new.quantity
# MAGIC   )

# COMMAND ----------

# MAGIC %md #### Movimentação de Arquivos Utilizados

# COMMAND ----------

for file in files:
    try:
        dbutils.fs.mv(file.path, '/FileStore/etl_pizzas/history/tb_fato/', True)
    except Exception as e:
        print('Erro!')