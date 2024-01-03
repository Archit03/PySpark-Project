import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max


class Spark:
    def __init__(self):
        self.spark = SparkSession.builder.appName("BreweryAnalysis").getOrCreate()

    def analyze_brewery_data(self, file_path):
        try:
            # Read the CSV file into a Spark DataFrame
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)

            # Calculate average alcohol content across all batches
            average_alcohol_content = df.select(avg('Alcohol_Content')).collect()[0][0]

            # Identify beer style with the highest alcohol content
            max_alcohol_content = df.agg(max('Alcohol_Content').alias('max_alcohol_content')).collect()[0][
                'max_alcohol_content']
            max_alcohol_beer_style = \
                df.filter(col('Alcohol_Content') == max_alcohol_content).select('Beer_Style').collect()[0][0]

            # Identify location with the highest total sales
            max_sales_location = \
                df.agg(max('Total_Sales').alias('max_sales')).join(df, col('max_sales') == col('Total_Sales')).select(
                    'Location').collect()[0][0]

            # Identify batch with the highest quality score
            max_quality_batch_id = df.agg(max('Quality_Score').alias('max_quality_score')).join(df,
                                                                                                col('max_quality_score') == col(
                                                                                                    'Quality_Score')).select(
                'Batch_ID').collect()[0][0]

            # Display batches with Alcohol Content greater than a certain threshold (e.g., 5%)
            high_alcohol_batches = df.filter(col('Alcohol_Content') > 5)

            # Calculate average sales per location
            avg_sales_per_location = df.groupBy('Location').agg(avg('Total_Sales').alias('Average_Sales'))

            return average_alcohol_content, max_alcohol_beer_style, max_sales_location, max_quality_batch_id, high_alcohol_batches, avg_sales_per_location

        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def stop(self):
        # Stop Spark session to release resources
        if 'spark' in locals():
            self.spark.stop()


# Streamlit App
def main():
    st.title("Brewery Data Analysis Dashboard")

    # Load data and perform analysis
    csv_path = "brewery_data_complete_extended.csv"

    # Initialize Spark session
    spark_instance = Spark()

    # Perform analysis
    (average_alcohol_content, max_alcohol_beer_style, max_sales_location,
     max_quality_batch_id, high_alcohol_batches, avg_sales_per_location) = spark_instance.analyze_brewery_data(csv_path)

    # Display Average Alcohol Content
    st.subheader("Average Alcohol Content:")
    st.write(f"Average Alcohol Content: {average_alcohol_content:.2f}%")

    # Display Beer Style with the Highest Alcohol Content
    st.subheader("Beer Style with the Highest Alcohol Content:")
    st.write(f"Beer Style with the Highest Alcohol Content: {max_alcohol_beer_style}")

    # Display Location with the Highest Total Sales
    st.subheader("Location with the Highest Total Sales:")
    st.write(f"Location with the Highest Total Sales: {max_sales_location}")

    # Display Batch with the Highest Quality Score
    st.subheader("Batch with the Highest Quality Score:")
    st.write(f"Batch with the Highest Quality Score: {max_quality_batch_id}")

    # Display Batches with Alcohol Content greater than a certain threshold
    st.subheader("Batches with Alcohol Content > 5%:")
    st.write(high_alcohol_batches)

    # Display Average Sales per Location
    st.subheader("Average Sales per Location:")
    st.write(avg_sales_per_location)

    # Visualize Average Sales per Location (Bar Chart)
    st.subheader("Average Sales per Location (Bar Chart)")
    bar_chart_data = avg_sales_per_location.toPandas()
    st.bar_chart(bar_chart_data.set_index('Location')['Average_Sales'])

    # Visualize Distribution of Beer Styles (Pie Chart)
    st.subheader("Distribution of Beer Styles (Pie Chart)")
    beer_style_distribution = spark_instance.df.groupBy('Beer_Style').count().toPandas()
    st.bar_chart(beer_style_distribution.set_index('Beer_Style')['count'])

    # Stop Spark session to release resources
    spark_instance.stop()


if __name__ == "__main__":
    main()
