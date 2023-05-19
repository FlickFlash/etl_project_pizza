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

df_orders_pizzas = spark.table('projeto_pizza.tb_silver_orders_pizzas')

# COMMAND ----------

# MAGIC %md #### Transformação e Adição de Colunas

# COMMAND ----------

map_pizza_size = {'S': '1', 'M':'2', 'L':'3', 'XL':'4', 'XXL':'5'}

# COMMAND ----------

df_orders_pizzas_ingredients = (df_orders_pizzas
                           .withColumn('ingredients', F.explode(F.split(F.col('ingredients'), ', ')))
                           .withColumn('ingredients_quantity', F.col('size'))
                           .replace(map_pizza_size, subset=['ingredients_quantity'])
                           .withColumn('ingredients_quantity', F.col('ingredients_quantity').cast('int'))
                           .withColumn('total_ingredients_quantity', F.col('quantity') * F.col('ingredients_quantity'))
                          )

# COMMAND ----------

# MAGIC %md #### Agrupamento por Métricas

# COMMAND ----------

df_quantity_ingredients = (df_orders_pizzas_ingredients
                           .groupBy('year_month', 'ingredients')
                           .agg(F.count_distinct('date').alias('qty_days_month'), F.sum('total_ingredients_quantity').alias('total_mensal'))
                           .withColumn('media_diaria', F.round(F.col('total_mensal')/F.col('qty_days_month'), 2))
                          )

# COMMAND ----------

df_quantity_type = (df_orders_pizzas_ingredients
                       .groupBy('year_month', 'name')
                       .agg(F.count_distinct('date').alias('qty_days_month'), F.count('pizza_type_id').alias('total_mensal'))
                       .withColumn('media_diaria', F.round(F.col('total_mensal')/F.col('qty_days_month'), 2))
                      )

# COMMAND ----------

df_quantity_size = (df_orders_pizzas_ingredients
                       .groupBy('year_month', 'size')
                       .agg(F.count_distinct('date').alias('qty_days_month'), F.count('pizza_type_id').alias('total_mensal'))
                       .withColumn('media_diaria', F.round(F.col('total_mensal')/F.col('qty_days_month'), 2))
                      )

# COMMAND ----------

df_quantity_category = (df_orders_pizzas_ingredients
                           .groupBy('year_month', 'category')
                           .agg(F.count_distinct('date').alias('qty_days_month'), F.count('category').alias('total_mensal'))
                           .withColumn('media_diaria', F.round(F.col('total_mensal')/F.col('qty_days_month'), 2))
                          )

# COMMAND ----------

# MAGIC %md ## Criação de Arquivos Parquet

# COMMAND ----------

df_quantity_size.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_quantity_size')
df_quantity_category.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_quantity_category')
df_quantity_type.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_quantity_type')
df_quantity_ingredients.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_quantity_ingredients')

# COMMAND ----------

# MAGIC %md ## Criação de Tabelas SQL

# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_quantity_size USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_quantity_size"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_quantity_category USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_quantity_category"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_quantity_type USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_quantity_type"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_quantity_ingredients USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_quantity_ingredients"')