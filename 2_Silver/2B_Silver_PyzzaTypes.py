# Databricks notebook source
# MAGIC %md ## Importação de Bibliotecas

# COMMAND ----------

import pyspark
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md ## Predefinições

# COMMAND ----------

BASE_FILE_PATH = '/FileStore/etl_pizzas/tb_dimensao'

# COMMAND ----------

files = dbutils.fs.ls(BASE_FILE_PATH)

# COMMAND ----------

# MAGIC %md ## Processamento

# COMMAND ----------

# MAGIC %md #### Leitura de arquivos

# COMMAND ----------

df_pizza_types = spark.table('projeto_pizza.tb_bronze_pizza_types')
df_pizzas = spark.table('projeto_pizza.tb_bronze_pizzas')

# COMMAND ----------

# MAGIC %md #### Transformação de Colunas

# COMMAND ----------

df_pizzas = df_pizzas.withColumn('price', F.col('price').cast('decimal(4,2)'))

# COMMAND ----------

df_pizzas = (df_pizzas.withColumn('pizza_id_array', F.split(F.col('pizza_id'), '_'))
                      .withColumn('size', F.upper(F.element_at(F.col('pizza_id_array'), -1)))
                      .drop('pizza_id_array'))

# COMMAND ----------

# MAGIC %md #### Join entre DataFrames

# COMMAND ----------

df_pizzas_detailed = df_pizzas.join(df_pizza_types, on=['pizza_type_id'], how='left')

# COMMAND ----------

# MAGIC %md #### Adição de Valores em Nova Coluna

# COMMAND ----------

# 'date_price' deve ser um valor adicionado automaticamente, tanto inicialmente, quanto durante processamentos posteriores
if spark.table('projeto_pizza.tb_silver_pizzas_detailed').isEmpty():
    df_pizzas_detailed = df_pizzas_detailed.withColumn('date_price', F.to_date(F.lit('2015-01-01'), 'yyyy-MM-dd'))
else:
    df_pizzas_detailed = df_pizzas_detailed.withColumn('date_price', F.to_date(F.lit(datetime.now().strftime('%Y-%m-%d')), 'yyyy-MM-dd'))


# COMMAND ----------

# MAGIC %md #### Realização do Upsert

# COMMAND ----------

df_pizzas_detailed.createOrReplaceTempView('vw_pizzas_detailed')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO projeto_pizza.tb_silver_pizzas_detailed AS old
# MAGIC USING vw_pizzas_detailed AS new
# MAGIC ON ((old.pizza_id = new.pizza_id) AND (old.date_price = new.date_price))
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET*
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT(
# MAGIC   old.pizza_type_id,
# MAGIC   old.pizza_id,
# MAGIC   old.size,
# MAGIC   old.price,
# MAGIC   old.name,
# MAGIC   old.category,
# MAGIC   old.ingredients,
# MAGIC   old.date_price
# MAGIC   )
# MAGIC   VALUES(
# MAGIC   new.pizza_type_id,
# MAGIC   new.pizza_id,
# MAGIC   new.size,
# MAGIC   new.price,
# MAGIC   new.name,
# MAGIC   new.category,
# MAGIC   new.ingredients,
# MAGIC   new.date_price
# MAGIC   )

# COMMAND ----------

# MAGIC %md #### Movimentação de Arquivos Utilizados

# COMMAND ----------

for file in files:
    try:
        dbutils.fs.mv(file.path, '/FileStore/etl_pizzas/history/tb_dimensao/', True)
    except Exception as e:
        print('Erro!')