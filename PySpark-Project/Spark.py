from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max


def analyze_brewery_data(file_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BreweryAnalysis") \
        .getOrCreate()

    # Read the CSV file into a Spark DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Show the PySpark DataFrame description
    print("DataFrame Description:")
    df.describe().show()

    # Calculate average alcohol content across all batches
    average_alcohol_content = df.select(avg('Alcohol_Content')).collect()[0][0]
    print(f"Average Alcohol Content: {average_alcohol_content:.2f}%")

    # Identify beer style with the highest alcohol content
    max_alcohol_content = df.agg(max('Alcohol_Content').alias('max_alcohol_content')).collect()[0][
        'max_alcohol_content']
    max_alcohol_beer_style = df.filter(col('Alcohol_Content') == max_alcohol_content).select('Beer_Style').collect()[0][
        0]
    print(f"Beer Style with the Highest Alcohol Content: {max_alcohol_beer_style}")

    # Identify location with the highest total sales
    max_sales_location = \
    df.agg(max('Total_Sales').alias('max_sales')).join(df, col('max_sales') == col('Total_Sales')).select(
        'Location').collect()[0][0]
    print(f"Location with the Highest Total Sales: {max_sales_location}")

    # Identify batch with the highest quality score
    max_quality_batch_id = df.agg(max('Quality_Score').alias('max_quality_score')).join(df,
                                                                                        col('max_quality_score') == col(
                                                                                            'Quality_Score')).select(
        'Batch_ID').collect()[0][0]
    print(f"Batch with the Highest Quality Score: {max_quality_batch_id}")

    # Display batches with Alcohol Content greater than a certain threshold (e.g., 5%)
    print("Batches with Alcohol Content Greater than 5%:")
    high_alcohol_batches = df.filter(col('Alcohol_Content') > 5)
    high_alcohol_batches.show()

    # Calculate average sales per location
    avg_sales_per_location = df.groupBy('Location').agg(avg('Total_Sales').alias('Average_Sales'))
    print("Average Sales per Location:")
    avg_sales_per_location.show()


# Usage
analyze_brewery_data("brewery_data_complete_extended.csv")
