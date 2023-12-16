from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
import Spark

# Assuming Spark.df is a PySpark DataFrame with columns "Total_Sales", "Volume_Produced"

                                                                                
df = Spark.df
df.select("Total_Sales").show()

# Drop the existing "_features" column


# Create VectorAssembler with a unique output column name
assembler = VectorAssembler(inputCols=["Total_Sales", "Volume_Produced"], outputCol="_features")

# Transform the DataFrame with VectorAssembler
df_assembled = assembler.transform(df).withColumnRenamed("_features", "features")

# Display the DataFrame with the assembled features
df_assembled.show(truncate=False)

# Split the data into training and testing sets
(train_data, test_data) = df_assembled.randomSplit([0.8, 0.2], seed=1234)

# Set "Total_Sales" as the label column
lr = LinearRegression(featuresCol="_features", labelCol="Total_Sales")

# Create a pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Train the model
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Display predictions
predictions.select("features", "Total_Sales", "prediction").show()

# If you want to use the assembler in a pipeline
# Create a new VectorAssembler with a consistent output column name
assembler_pipeline = VectorAssembler(inputCols=["Total_Sales", "Volume_Produced"], outputCol="__features")

# Set "Total_Sales" as the label column
lr_pipeline = LinearRegression(featuresCol="__features", labelCol="Total_Sales", regParam=0.01)

# Create a pipeline
pipeline = Pipeline(stages=[assembler_pipeline, lr_pipeline])

# Fit the pipeline
model_pipeline = pipeline.fit(df)

# Transform the DataFrame using the fitted pipeline
df_transformed = model_pipeline.transform(df)

# Display the transformed DataFrame
df_transformed.select("__features", "Total_Sales", "prediction").show()
