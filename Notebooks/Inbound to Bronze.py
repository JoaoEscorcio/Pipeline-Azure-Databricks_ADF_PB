# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Check access point

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/inbound")

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------


actor = spark.read.csv("dbfs:/mnt/dados/inbound/actor_202403130942.csv", sep=',', inferSchema=True, header=True)


print("The number of actor is:", actor.count())



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Uploading all tables

# COMMAND ----------

address = spark.read.csv("dbfs:/mnt/dados/inbound/address_202403130942.csv", sep=',', inferSchema=True, header=True)
category = spark.read.csv("dbfs:/mnt/dados/inbound/category_202403130942.csv" , sep=',', inferSchema=True, header=True)
city = spark.read.csv("dbfs:/mnt/dados/inbound/city_202403130942.csv" , sep=',', inferSchema=True, header=True)
country = spark.read.csv("dbfs:/mnt/dados/inbound/country_202403130943.csv" , sep=',', inferSchema=True, header=True)
customer = spark.read.csv("dbfs:/mnt/dados/inbound/customer_202403130943.csv" , sep=',', inferSchema=True, header=True)
film = spark.read.csv("dbfs:/mnt/dados/inbound/film_202403130943.csv" , sep=',', inferSchema=True, header=True)
film_actor = spark.read.csv("dbfs:/mnt/dados/inbound/film_actor_202403130943.csv" , sep=',', inferSchema=True, header=True)
film_category = spark.read.csv("dbfs:/mnt/dados/inbound/film_category_202403130944.csv" , sep=',', inferSchema=True, header=True)
film_text = spark.read.csv("dbfs:/mnt/dados/inbound/film_text_202403130944.csv" , sep=',', inferSchema=True, header=True)
inventory = spark.read.csv("dbfs:/mnt/dados/inbound/inventory_202403130944.csv" , sep=',', inferSchema=True, header=True)
payment = spark.read.csv("dbfs:/mnt/dados/inbound/payment_202403130944.csv" , sep=',', inferSchema=True, header=True)
rental = spark.read.csv("dbfs:/mnt/dados/inbound/rental_202403130944.csv" , sep=',', inferSchema=True, header=True)
staff = spark.read.csv("dbfs:/mnt/dados/inbound/staff_202403130945.csv" , sep=',', inferSchema=True, header=True)
store = spark.read.csv("dbfs:/mnt/dados/inbound/store_202403130945.csv" , sep=',', inferSchema=True, header=True)
language = spark.read.csv("dbfs:/mnt/dados/inbound/language", sep=',', inferSchema=True, header=True)

print("The number of actor is:", actor.count())
print("The number of address is:", address.count())
print("The number of category is:", category.count())
print("The number of city is:", city.count())
print("The number of country is:", country.count())
print("The number of customer is:", customer.count())
print("The number of film is:", film.count())
print("The number of film_actor is:", film_actor.count())
print("The number of film_category is:", film_category.count())
print("The number of film_text is:", film_text.count())
print("The number of inventory is:", inventory.count())
print("The number of payment is:", payment.count())
print("The number of rental is:", rental.count())
print("The number of staff is:", staff.count())
print("The number of store is:", store.count())
print("The number of languages is:", language.count())


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definition function that saves and transforms dimensions

# COMMAND ----------

def processar_tabela_e_salvar_delta(tabela, caminho_delta):

    # Salvar a tabela em formato Delta
    tabela.write.format("delta").mode("overwrite").save(caminho_delta)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Denormalization stage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Costumer Dimension

# COMMAND ----------

dbutils.fs.ls("mnt/dados/")

# COMMAND ----------

# Renomear colunas da tabela customer para evitar ambiguidade
customer = customer.withColumnRenamed("last_update", "customer_last_update")

# Renomear colunas da tabela address para evitar ambiguidade
address = address.withColumnRenamed("last_update", "address_last_update")

# Fazer join entre as tabelas usando a chave estrangeira address_id
dim_customer = customer.join(address, "address_id")

# Selecionar as colunas desejadas
dim_customer = dim_customer.select("customer_id", "first_name", "last_name", "email", "active", "create_date", "customer_last_update",
                                   "address_id", "address", "address2", "district", "city_id", "postal_code", "phone")

# Adicionar uma coluna de identificação baseada no termo do nome da tabela
dim_customer = dim_customer.withColumn("costumer_key", monotonically_increasing_id())
    
# Adicionar uma coluna de timestamp
dim_customer = dim_customer.withColumn("insert_data_timestamp", current_timestamp())

caminho_delta_destino = "dbfs:/mnt/dados/bronze/dim_customer_delta"

    
processar_tabela_e_salvar_delta(dim_customer, caminho_delta_destino)

# COMMAND ----------

dim_customer.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store Dimension

# COMMAND ----------

# Fazer joins entre as tabelas usando as chaves estrangeiras
dim_store = store.join(address, "address_id") \
    .join(city, "city_id") \
    .join(country, "country_id")


# Lista das colunas a serem mantidas
colunas_mantidas = ["store_id", "address", "address2", "postal_code", "phone", "address_last_update", "city","country"]

# Dropar as colunas indesejadas
dim_store = dim_store.drop(*[coluna for coluna in dim_store.columns if coluna not in colunas_mantidas])

# COMMAND ----------

# Adicionar uma coluna de identificação baseada no termo do nome da tabela
dim_store = dim_store.withColumn("store_key", monotonically_increasing_id())
    
# Adicionar uma coluna de timestamp
dim_store = dim_store.withColumn("insert_data_timestamp", current_timestamp())

caminho_delta_destino = "dbfs:/mnt/dados/bronze/dim_store_delta"

    
processar_tabela_e_salvar_delta(dim_store, caminho_delta_destino)

dim_store.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Movie Dimension

# COMMAND ----------

# Fazer join com a tabela language para obter o nome do idioma
dim_movie = film.join(language, "language_id") \
    .withColumnRenamed("name", "language")

# Fazer join com a tabela film_category para obter a categoria
dim_movie = dim_movie.join(film_category, "film_id") \
    .join(category, "category_id") \
    .withColumnRenamed("name", "category")

# Lista das colunas a serem mantidas
colunas_mantidas = [
    "film_id",
    "title",
    "description",
    "release_year",
    "rental_duration",
    "length",
    "rating",
    "special_features",
    "language",
    "category"
]

# Dropar as colunas indesejadas
dim_movie = dim_movie.drop(*[coluna for coluna in dim_movie.columns if coluna not in colunas_mantidas])

# COMMAND ----------

# Adicionar uma coluna de identificação baseada no termo do nome da tabela
dim_movie = dim_movie.withColumn("movie_key", monotonically_increasing_id())
    
# Adicionar uma coluna de timestamp
dim_movie = dim_movie.withColumn("insert_data_timestamp", current_timestamp())

caminho_delta_destino = "dbfs:/mnt/dados/bronze/dim_movie_delta"

    
processar_tabela_e_salvar_delta(dim_movie, caminho_delta_destino)

# Visualizar o esquema resultante
dim_movie.printSchema()

# COMMAND ----------

dim_movie.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Time Dimension

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, quarter, month, dayofmonth, dayofweek, expr
# Selecionar as datas distintas da coluna rental_date
distinct_dates = rental.select(col("rental_date").cast("date").alias("date")).distinct()

# Criar a dimensão de tempo com as colunas desejadas e a coluna "Isweekend"
dim_time = distinct_dates.withColumn("year", year("date")) \
                         .withColumn("quarter", quarter("date")) \
                         .withColumn("month", month("date")) \
                         .withColumn("day", dayofmonth("date")) \
                         .withColumn("day_of_week", dayofweek("date")) \
                         .withColumn("Isweekend", expr("dayofweek(date) in (1, 7)"))


# COMMAND ----------

# Adicionar uma coluna de identificação baseada no termo do nome da tabela
dim_time = dim_time.withColumn("time_key", monotonically_increasing_id())
    
# Adicionar uma coluna de timestamp
dim_time = dim_time.withColumn("insert_data_timestamp", current_timestamp())

caminho_delta_destino = "dbfs:/mnt/dados/bronze/dim_time_delta"

    
processar_tabela_e_salvar_delta(dim_time, caminho_delta_destino)

# Exibir o DataFrame resultante
dim_time.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fact

# COMMAND ----------

# Join the tables to obtain the necessary values
sales_fact = dim_customer.join(rental, "customer_id") \
                         .join(payment, rental["rental_id"] == payment["rental_id"]) \
                         .join(inventory, "inventory_id") \
                         .join(dim_movie, inventory["film_id"] == dim_movie["film_id"]) \
                         .join(dim_store, inventory["store_id"] == dim_store["store_id"]) \
                         .select(dim_store["store_key"].alias("store_key"),
                                 dim_movie["movie_key"].alias("movie_key"),
                                 dim_customer["costumer_key"].alias("costumer_key"),
                                 rental["rental_id"],
                                 payment["amount"].alias ("Sales_amount"))

# COMMAND ----------

sales_fact.show()

# COMMAND ----------

# Adicionar uma coluna de identificação baseada no termo do nome da tabela
sales_fact = sales_fact.withColumn("fact_key", monotonically_increasing_id())
    
# Adicionar uma coluna de timestamp
sales_fact = sales_fact.withColumn("insert_data_timestamp", current_timestamp())

caminho_delta_destino = "dbfs:/mnt/dados/bronze/sales_fact_delta"

    
processar_tabela_e_salvar_delta(sales_fact, caminho_delta_destino)

# Exibir o DataFrame resultante
sales_fact.show()

# COMMAND ----------

sales_fact.printSchema()

# COMMAND ----------


