# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/shopping_behavior_updated.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "shopping_behavior_updated_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `shopping_behavior_updated_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "shopping_behavior_updated_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# Assuming you're reading a CSV file
file_path ="/FileStore/tables/shopping_behavior_updated.csv"

# Read the data and specify that the first row should be used as the header
df = spark.read.option("header", "true").csv(file_path)

# Now you can display the DataFrame
display(df)


# COMMAND ----------

# Count the number of unique customers
unique_customers = df.select('Customer ID').distinct().count()
print(f"Total unique customers: {unique_customers}")


# COMMAND ----------

from pyspark.sql.functions import col

# Assuming you have loaded your dataset into a DataFrame named 'df'

# Convert 'Purchase Amount (USD)' to a float
df = df.withColumn('Purchase Amount (USD)', df['Purchase Amount (USD)'].cast('float'))

# Group by Category and calculate the average purchase amount
category_avg_purchase = df.groupBy('Category').agg({'Purchase Amount (USD)': 'avg'})

# Rename the resulting column to 'Average Purchase Amount'
category_avg_purchase = category_avg_purchase.withColumnRenamed('avg(Purchase Amount (USD))', 'Average Purchase Amount')

# Display the result
display(category_avg_purchase)


# COMMAND ----------

# Identify the top locations with the highest total purchase amounts
top_locations = df.groupBy('Location').agg({'Purchase Amount (USD)': 'sum'}).orderBy(col('sum(Purchase Amount (USD))').desc())
display(top_locations)

# Analyze how the average purchase amount varies by location
location_avg_purchase = df.groupBy('Location').agg({'Purchase Amount (USD)': 'avg'})
display(location_avg_purchase)




# COMMAND ----------



# COMMAND ----------

# Calculate the total purchase amount for each season
seasonal_purchase_total = df.groupBy('Season').agg({'Purchase Amount (USD)': 'sum'})
display(seasonal_purchase_total)

# Determine which season has the highest sales
highest_sales_season = seasonal_purchase_total.orderBy(col('sum(Purchase Amount (USD))').desc()).first()[0]
print(f"Season with Highest Sales: {highest_sales_season}")




# COMMAND ----------



# COMMAND ----------

# Find out which colors and sizes are most popular among customers
popular_colors = df.groupBy('Color').count().orderBy(col('count').desc()).first()[0]
popular_sizes = df.groupBy('Size').count().orderBy(col('count').desc()).first()[0]
print(f"Most Popular Color: {popular_colors}")
print(f"Most Popular Size: {popular_sizes}")




# COMMAND ----------

# Determine the most popular product or item purchased
most_popular_product = df.groupBy('Item Purchased').count().orderBy(col('count').desc()).first()[0]
print(f"Most Popular Product: {most_popular_product}")

# Analyze which categories have the highest and lowest average purchase amounts
category_avg_purchase = df.groupBy('Category').agg({'Purchase Amount (USD)': 'avg'})
display(category_avg_purchase)

# Visualize the distribution of purchases by product category
category_purchase_distribution = df.groupBy('Category').count()
display(category_purchase_distribution)

