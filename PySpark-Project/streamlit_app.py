import streamlit as st
from Spark import analyze_brewery_data  # Assuming your function is named analyze_brewery_data


def main():
    st.title("Brewery Data Analysis")

    # Load data and perform analysis
    csv_path = "brewery_data_complete_extended.csv"
    result = analyze_brewery_data(csv_path)

    descriptive_stats, average_alcohol_content, max_alcohol_beer_style, max_sales_location, max_quality_batch_id, high_alcohol_batches, avg_sales_per_location = result

    # Display Descriptive Statistics
    st.subheader("Descriptive Statistics:")
    st.write(descriptive_stats)

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


if __name__ == "__main__":
    main()
