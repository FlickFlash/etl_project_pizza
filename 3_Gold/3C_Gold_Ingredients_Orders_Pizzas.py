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

df_ingredients_size = (df_orders_pizzas_ingredients
                       .groupBy('year_month', 'size', 'ingredients')
                       .agg(F.count_distinct('date').alias('qty_days_month'), F.sum('total_ingredients_quantity').alias('sum_ingredients_quantity'))
                       .withColumn('media_diaria', F.round(F.col('sum_ingredients_quantity')/F.col('qty_days_month'), 2))
                      ).orderBy(F.col('year_month').asc(), F.col('media_diaria').desc())#, F.col('total_ingredients').desc())

# COMMAND ----------

df_ingredients_type = (df_orders_pizzas_ingredients.groupBy('year_month', 'name', 'ingredients')
                       .agg(F.count_distinct('date').alias('qty_days_month'), F.sum('total_ingredients_quantity').alias('sum_ingredients_quantity'))
                       .withColumn('media_diaria', F.round(F.col('sum_ingredients_quantity')/F.col('qty_days_month'), 2))
                      ).orderBy(F.col('year_month').asc(), F.col('media_diaria').desc())

# COMMAND ----------

df_ingredients_category = (df_orders_pizzas_ingredients
                           .groupBy('year_month', 'category', 'ingredients')
                           .agg(F.count_distinct('date').alias('qty_days_month'), F.sum('total_ingredients_quantity').alias('sum_ingredients_quantity'))
                           .withColumn('media_diaria', F.round(F.col('sum_ingredients_quantity')/F.col('qty_days_month'), 2))
                      ).orderBy(F.col('year_month').asc(), F.col('media_diaria').desc())

# COMMAND ----------

df_ingredients_total = (df_orders_pizzas_ingredients
                           .groupBy('year_month', 'ingredients')
                           .agg(F.count_distinct('date').alias('qty_days_month'), F.sum('total_ingredients_quantity').alias('sum_ingredients_quantity'))
                           .withColumn('media_diaria', F.round(F.col('sum_ingredients_quantity')/F.col('qty_days_month'), 2))
                      ).orderBy(F.col('year_month').asc(), F.col('media_diaria').desc())

# COMMAND ----------

# MAGIC %md ## Criação de Arquivos Parquet

# COMMAND ----------

df_ingredients_size.write.mode('overwrite').option('overwiteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_ingredients_size')
df_ingredients_category.write.mode('overwrite').option('overwiteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_ingredients_category')
df_ingredients_type.write.mode('overwrite').option('overwiteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_ingredients_type')
df_ingredients_total.write.mode('overwrite').option('overwiteSchema', 'True').save(BASE_DB_PATH + '/gold/tb_gold_ingredients_total')

# COMMAND ----------

# MAGIC %md ## Criação de Tabelas SQL

# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_ingredients_size USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_ingredients_size"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_ingredients_category USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_ingredients_category"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_ingredients_type USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_ingredients_type"')
spark.sql(f'CREATE TABLE IF NOT EXISTS projeto_pizza.tb_gold_ingredients_total USING delta LOCATION "{BASE_DB_PATH}/gold/tb_gold_ingredients_total"')