# Databricks notebook source
# MAGIC %md ## Importação de Bibliotecas

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ## Predefinições

# COMMAND ----------

BASE_DB_PATH  = '/FileStore/etl_pizzas/database/'

# COMMAND ----------

# MAGIC %md ## Processamento

# COMMAND ----------

# MAGIC %md #### Leitura de arquivos

# COMMAND ----------

df_orders_detailed = spark.table('projeto_pizza.tb_silver_orders_detailed')

# COMMAND ----------

df_pizzas_detailed = spark.table('projeto_pizza.tb_silver_pizzas_detailed')

# COMMAND ----------

# MAGIC %md #### Join entre DataFrames

# COMMAND ----------

# Data do pedido precisa ser >= a data do preço
df_orders_pizzas = (df_orders_detailed
                    .join(df_pizzas_detailed, on=((df_orders_detailed['pizza_id'] == df_pizzas_detailed['pizza_id']) & (df_orders_detailed['date'] >= df_pizzas_detailed['date_price'])), how='left')
                    .drop(df_pizzas_detailed['pizza_id'])
                    .withColumn('year_month', F.date_format(F.col('date'), 'yyyy-MM'))
                        )

# COMMAND ----------

# MAGIC %md #### Seleção de Linhas com "Datas Válidas"

# COMMAND ----------

windowSpec = Window.partitionBy('order_details_id').orderBy(F.col('date_price').desc())

# COMMAND ----------

df_orders_pizzas = df_orders_pizzas.withColumn('rank', F.rank().over(windowSpec)).filter(F.col('rank') == 1).drop('rank')

# COMMAND ----------

# MAGIC %md ## Criação de Arquivos Parquet

# COMMAND ----------

df_orders_pizzas.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/silver/tb_silver_orders_pizzas')

# COMMAND ----------

# MAGIC %md ## Criação de Tabelas SQL

# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_silver_orders_pizzas USING delta LOCATION "{BASE_DB_PATH}/silver/tb_silver_orders_pizzas"')