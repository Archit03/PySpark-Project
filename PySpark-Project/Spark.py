from pyspark.sql import SparkSession as sk
from pyspark.sql.functions import col, avg, max, min

# Initialize Spark session
spark = sk.builder \
    .appName("BreweryAnalysis") \
    .getOrCreate()

# Read the CSV file into a Pandas DataFrame
df = spark.read.csv("C:\\Users\\LENOVO\\Desktop\\New "
                    "folder\\PySpark-Project\\PySpark-Project\\brewery_data_complete_extended.csv", header=True,
                    inferSchema=True)

# Show the PySpark DataFrame
df.describe().show()

# Average alcohol content across all batches
average_alcohol_content = df.select(avg('Alcohol_Content')).collect()[0][0]
print(f"Average Alcohol Content: {average_alcohol_content:.2f}%")

# Beer style with the highest alcohol content using a subquery
max_alcohol_content = df.agg(max('Alcohol_Content').alias('max_alcohol_content')).collect()[0]['max_alcohol_content']
max_alcohol_beer_style = df.filter(col('Alcohol_Content') == max_alcohol_content).select('Beer_Style').collect()[0][0]
print(f"Beer Style with the Highest Alcohol Content: {max_alcohol_beer_style}")

# Location with the highest total sales
max_sales_location = df.agg(max('Total_Sales').alias('max_sales')).join(df, col('max_sales') == col('Total_Sales')).select('Location').collect()[0][0]
print(f"Location with the Highest Total Sales: {max_sales_location}")

# Batch with the highest quality score
max_quality_batch_id = df.agg(max('Quality_Score').alias('max_quality_score')).join(df, col('max_quality_score') == col('Quality_Score')).select('Batch_ID').collect()[0][0]
print(f"Batch with the Highest Quality Score: {max_quality_batch_id}")

# Batches with Alcohol Content greater than a certain threshold (e.g., 5%)
high_alcohol_batches = df.filter(col('Alcohol_Content') > 5)
high_alcohol_batches.show()

# More advanced analytics can involve joining with other DataFrames, grouping, and aggregating data based on specific
#  criteria. Finding average sales per location:
avg_sales_per_location = df.groupBy('Location').agg(avg('Total_Sales').alias('Average_Sales'))
avg_sales_per_location.show()
