# Databricks notebook source
# MAGIC %md ## Configuração de Diretórios

# COMMAND ----------

try:
    dbutils.fs.mkdirs('/FileStore/etl_pizzas/tb_fato/')
except Exception as e:
    print(e)

# COMMAND ----------

try:
    dbutils.fs.mkdirs('/FileStore/etl_pizzas/history/tb_fato/')
except Exception as e:
    print(e)

# COMMAND ----------

try:
    dbutils.fs.mkdirs('/FileStore/etl_pizzas/tb_dimensao/')
except Exception as e:
    print(e)

# COMMAND ----------

try:
    dbutils.fs.mkdirs('/FileStore/etl_pizzas/history/tb_dimensao/')
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md ## Configuração de Tabelas

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC CREATE SCHEMA IF NOT EXISTS projeto_pizza

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS projeto_pizza.tb_silver_orders_detailed;
# MAGIC CREATE TABLE projeto_pizza.tb_silver_orders_detailed(
# MAGIC order_id string,
# MAGIC date date,
# MAGIC time string,
# MAGIC order_details_id string,
# MAGIC pizza_id string,
# MAGIC quantity int
# MAGIC )
# MAGIC USING DELTA LOCATION '/FileStore/etl_pizzas/database/silver/tb_silver_orders_detailed'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS projeto_pizza.tb_silver_pizzas_detailed;
# MAGIC CREATE TABLE projeto_pizza.tb_silver_pizzas_detailed(
# MAGIC pizza_type_id string,
# MAGIC pizza_id string,
# MAGIC size string,
# MAGIC price decimal(4,2),
# MAGIC name string,
# MAGIC category string,
# MAGIC ingredients string,
# MAGIC date_price date
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/etl_pizzas/database/silver/tb_silver_pizzas_detailed'