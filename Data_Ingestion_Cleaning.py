# Databricks notebook source
from pyspark.sql.functions import col, when, trim, lower, coalesce
from pyspark.sql.types import IntegerType, FloatType, StringType

# COMMAND ----------

df_raw = spark.read.table("smartwatch_raw")
display(df_raw.head(10))
print(f"Total rows: {df_raw.count()}")
print(f"Total columns: {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, when, trim, lower
from pyspark.sql.types import IntegerType, FloatType

# COMMAND ----------

df_cleaned = df_raw.dropDuplicates()

# COMMAND ----------

df_raw.createOrReplaceTempView("smartwatch_temp")
spark.sql("SELECT COUNT(*) as total FROM smartwatch_temp").show()

# COMMAND ----------

df_cleaned = df_cleaned.fillna({
    'Price': 1500,
    'Battery_Life': 7,
    'Water_Resistance': 0,
    'Operating_System': 'Unknown'
})

# COMMAND ----------

df_cleaned = df_cleaned.withColumn('Price', col('Price').cast(IntegerType())) \
                        .withColumn('Battery_Life', col('Battery_Life').cast(FloatType())) \
                        .withColumn('Display_Size', col('Display_Size').cast(FloatType()))

# COMMAND ----------

df_cleaned = df_cleaned.filter((col('Price') > 0) & (col('Battery_Life') > 0))

# COMMAND ----------

df_cleaned = df_cleaned.withColumn('Brand', trim(lower(col('Brand'))))

# COMMAND ----------

# After loading df_raw, rename columns to remove spaces
df_raw = spark.read.table("smartwatch_raw")

# Rename columns: replace spaces with underscores
df_renamed = df_raw \
    .withColumnRenamed("Operating System", "Operating_System") \
    .withColumnRenamed("Display Size", "Display_Size") \
    .withColumnRenamed("Call Function", "Call_Function") \
    .withColumnRenamed("Display_Type", "Display_Type") \
    .withColumnRenamed("Display_Resolution", "Display_Resolution") \
    .withColumnRenamed("Water_Resistance", "Water_Resistance") \
    .withColumnRenamed("Battery_Life", "Battery_Life") \
    .withColumnRenamed("Model Name", "Model_Name")

# Now your original code will work perfectly!
df_cleaned = df_renamed.dropDuplicates()
# ... rest of your cleaning code ...


# COMMAND ----------

display(df_cleaned.limit(20))
print(f"Cleaned rows: {df_cleaned.count()}")

# COMMAND ----------

from pyspark.sql.functions import col, when

# Load cleaned data
df_cleaned = spark.read.table("smartwatch_silver")

# Create GOLD layer with categories
df_gold = df_cleaned.withColumn('Price_Category',
    when(col('Price') < 1000, 'Budget')
    .when(col('Price') < 3000, 'Mid-Range')
    .otherwise('Premium')
).withColumn('Battery_Category',
    when(col('Battery_Life') >= 10, 'Excellent')
    .when(col('Battery_Life') >= 7, 'Good')
    .when(col('Battery_Life') >= 5, 'Average')
    .otherwise('Poor')
)

print(f"GOLD layer created: {df_gold.count()} rows")
display(df_gold.limit(10))


# COMMAND ----------

from pyspark.sql.functions import col, when

# Load cleaned data
df_cleaned = spark.read.table("smartwatch_silver")

# Create GOLD layer with categories
df_gold = df_cleaned.withColumn('Price_Category',
    when(col('Price') < 1000, 'Budget')
    .when(col('Price') < 3000, 'Mid-Range')
    .otherwise('Premium')
).withColumn('Battery_Category',
    when(col('Battery_Life') >= 10, 'Excellent')
    .when(col('Battery_Life') >= 7, 'Good')
    .when(col('Battery_Life') >= 5, 'Average')
    .otherwise('Poor')
)

print(f"GOLD layer created: {df_gold.count()} rows")
display(df_gold.limit(10))


# COMMAND ----------

# Save SILVER (if not already saved)
df_cleaned = spark.read.table("smartwatch_silver")
df_cleaned.write.format("delta").mode("overwrite").saveAsTable("smartwatch_silver")
print("✓ SILVER table saved")

# Save GOLD
df_gold = spark.read.table("smartwatch_silver").withColumn('Price_Category',
    when(col('Price') < 1000, 'Budget')
    .when(col('Price') < 3000, 'Mid-Range')
    .otherwise('Premium')
).withColumn('Battery_Category',
    when(col('Battery_Life') >= 10, 'Excellent')
    .when(col('Battery_Life') >= 7, 'Good')
    .when(col('Battery_Life') >= 5, 'Average')
    .otherwise('Poor')
)

df_gold.write.format("delta").mode("overwrite").saveAsTable("smartwatch_gold")
print("✓ GOLD table saved")

# Verify
spark.sql("SHOW TABLES").show()


# COMMAND ----------

df_gold = spark.sql('''
SELECT 
    Brand, Model_Name, Operating_System, Price, Battery_Life, Display_Size,
    Call_Function, Bluetooth, Wi_Fi, GPS, Display_Type,
    CASE 
        WHEN Price < 1000 THEN "Budget"
        WHEN Price < 3000 THEN "Mid-Range"
        ELSE "Premium"
    END as Price_Category,
    CASE
        WHEN Battery_Life >= 10 THEN "Excellent"
        WHEN Battery_Life >= 7 THEN "Good"
        WHEN Battery_Life >= 5 THEN "Average"
        ELSE "Poor"
    END as Battery_Category
FROM smartwatch_silver
''')

df_gold.write.format("delta").mode("overwrite").saveAsTable("smartwatch_gold")
