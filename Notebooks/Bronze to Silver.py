# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC Conferindo acesso

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/bronze")


# COMMAND ----------


def processar_tabela_e_salvar_delta(tabela, caminho_delta):

    # Salvar a tabela em formato Delta
    tabela.write.format("delta").mode("overwrite").save(caminho_delta)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Aggregating the rental value per customer

# COMMAND ----------

path = 'dbfs:/mnt/dados/bronze/sales_fact_delta/'

sales_fact = spark.read.format("delta").load(path)

# COMMAND ----------


from pyspark.sql.functions import *

# Group by costumer_key and calculate the sum of Sales_amount
# Then sort in descending order by the sum of sales
sales_agg = sales_fact.groupBy("costumer_key") \
                            .agg(sum("Sales_amount").alias("total_sales_amount")) \
                            .orderBy("total_sales_amount", ascending=False)


sales_agg = sales_agg.withColumn("total_sales_amount", format_number("total_sales_amount", 2))

# Show the result
sales_agg.show()


# COMMAND ----------

dbutils.fs.ls("/mnt/dados/inbound")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### How many days to rent and whether it's a weekend or not

# COMMAND ----------

path = 'dbfs:/mnt/dados/inbound/rental_202403130944.csv'
rental = spark.read.load(path, format='csv', header=True, inferSchema=True)


# COMMAND ----------

# Juntar sales_fact e rental usando a coluna rental_id
joined_sales_rental = sales_fact.join(rental, "rental_id", "inner")
display(joined_sales_rental)

# COMMAND ----------

# Adicionar coluna Isweekend
joined_sales_rental = joined_sales_rental.withColumn("Isweekend", expr("dayofweek(rental_date) in (1, 7)"))

# Adicionar coluna de diferença de datas (em dias)
joined_sales_rental = joined_sales_rental.withColumn("rental_duration_days", expr("datediff(return_date, rental_date)"))

# Exibir o DataFrame resultante
display(joined_sales_rental)

# COMMAND ----------

joined_sales_rental.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# Selecionar apenas as colunas desejadas
joined_sales_rental = joined_sales_rental.select(
    col("movie_key"),
    col("Sales_amount"),
    col("fact_key"),
    col("customer_id"),
    col("Isweekend"),
    col("rental_duration_days")
)

# Exibir o DataFrame resultante
display(joined_sales_rental)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Saving as delta

# COMMAND ----------

caminho_delta_destino = "dbfs:/mnt/dados/silver/joined_sales_rental"

    
processar_tabela_e_salvar_delta(joined_sales_rental, caminho_delta_destino)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Attaching the movie category

# COMMAND ----------

path = 'dbfs:/mnt/dados/bronze/dim_movie_delta/'

dim_movie = spark.read.format("delta").load(path)

# COMMAND ----------

dim_movie.printSchema()

# COMMAND ----------

joined_movies = joined_sales_rental.join(dim_movie, "movie_key", "inner")

display(joined_movies)

# COMMAND ----------

joined_movies.printSchema()

# COMMAND ----------

# Selecionar apenas as colunas desejadas
joined_movies = joined_movies.select(
    col("fact_key"),
    col("title"),
    col("language"),
    col("category"),
    col("Isweekend"),
    col("rental_duration_days"),
    col("Sales_amount")
)

# Exibir o DataFrame resultante
display(joined_movies)


# COMMAND ----------

display(joined_movies)

# COMMAND ----------

caminho_delta_destino = "dbfs:/mnt/dados/silver/joined_movies"

    
processar_tabela_e_salvar_delta(joined_movies, caminho_delta_destino)

# COMMAND ----------


