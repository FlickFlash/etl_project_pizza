# Databricks notebook source
# MAGIC %md ## Importação de Bibliotecas

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Predefinições

# COMMAND ----------

BASE_DB_PATH  = '/FileStore/etl_pizzas/database/'

# COMMAND ----------

# MAGIC %md ## Processamento

# COMMAND ----------

# MAGIC %md #### Leitura de arquivos

# COMMAND ----------

df_orders_pizzas = spark.table(f'projeto_pizza.tb_silver_orders_pizzas')

# COMMAND ----------

# MAGIC %md #### Agrupamento por Métricas

# COMMAND ----------

df_faturamento_type = (df_orders_pizzas
                             .groupBy('year_month', 'name')
                             .agg(F.count_distinct('date').alias('qty_days_month'), F.sum('price').alias('total_mensal'))
                             .withColumn('media_diaria', F.round(F.col('total_mensal')/F.col('qty_days_month'), 2))
                            )

# COMMAND ----------

df_faturamento_size = (df_orders_pizzas
                             .groupBy('year_month', 'size')
                             .agg(F.count_distinct('date').alias('qty_days_month'), F.sum('price').alias('total_mensal'))
                             .withColumn('media_diaria', F.round(F.col('total_mensal')/F.col('qty_days_month'), 2))
                            )

# COMMAND ----------

df_faturamento_category = (df_orders_pizzas
                             .groupBy('year_month', 'category')
                             .agg(F.count_distinct('date').alias('qty_days_month'), F.sum('price').alias('total_mensal'))
                             .withColumn('media_diaria', F.round(F.col('total_mensal')/F.col('qty_days_month'), 2))
                            )

# COMMAND ----------

# MAGIC %md ## Criação de Arquivos Parquet

# COMMAND ----------

df_faturamento_type.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_faturamento_type')
df_faturamento_size.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_faturamento_size')
df_faturamento_category.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_faturamento_category')

# COMMAND ----------

# MAGIC %md ## Criação de Tabelas SQL

# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_faturamento_type USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_faturamento_type"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_faturamento_size USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_faturamento_size"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_faturamento_category USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_faturamento_category"')