# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Data Wrangling Walkthrough
# MAGIC
# MAGIC This notebook provides examples of several core PySpark operations using the Spark dataframe.  This will include:
# MAGIC - Reading data from CSV
# MAGIC - Basic dataframe metrics
# MAGIC - Joining data 
# MAGIC - Melting dataframes
# MAGIC - Filtering data
# MAGIC - Aggregation operations
# MAGIC - Saving data
# MAGIC
# MAGIC

# COMMAND ----------

# Replace cooplakehoues with the name of your storage account.
storage_end_point = "cooplakehouse.dfs.core.windows.net"

# Use the name of the secret scope that you set up along with the name of the secret in the key vault containing the storage account access key.
my_scope = "coop-lakehouse-scope"
my_key = "cooplakehouse-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key)
)

# Replace the container name (lakehouseblogstore) and storage account name (cooplakehouse) in the uri.
uri = "abfss://lakehouseblobstore@cooplakehouse.dfs.core.windows.net/TrainingFiles/"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading data
# MAGIC
# MAGIC This section will provide a couple of examples of reading data into a notebook.
# MAGIC

# COMMAND ----------

# Read the Grades file using defaults and use the top row as header (not the default behavior)
grades_df = spark.read.csv(uri+"Grades", header=True)
 
display(grades_df)



# COMMAND ----------

# Columns, number of rows and columns, data types.
print(grades_df.columns)  

print("Total columns:  ", len(grades_df.columns))
print("Total rows:  ", grades_df.count())   

print(grades_df.dtypes)



# COMMAND ----------

# Read the Students file utilizing schema on read and specifying more parameters.
# For more background on this, see https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/.

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema = StructType([StructField("StudentID", IntegerType(), True), \
                    StructField("Major", StringType(), True), \
                    StructField("HomeState", StringType(), True) ])
students_df = spark.read.options(delimiter=',', header=True).schema(schema).csv(uri+"StudentInfo.csv")

display(students_df)


# COMMAND ----------

# Columns, number of rows and columns, data types.
print(students_df.columns)  

print("Total columns:  ", len(students_df.columns))
print("Total rows:  ", students_df.count())   

print(students_df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Join the two data sets
# MAGIC
# MAGIC You should be familiar with the various types of joins.  We will use an inner join to get the student information together with the grades.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import IntegerType

# Convert the StudentID to an integer in grades to match students.
grades_df = grades_df.withColumn('StudentID', grades_df.StudentID.cast(IntegerType()))


# COMMAND ----------

grades_student_df = grades_df.join(students_df, on='StudentID', how='inner')

display(grades_student_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select a subset of the columns - only student info, tests, and final.

# COMMAND ----------

tests_df = grades_student_df.select('StudentID', 'Major', 'HomeState', 'Test1', 'Test2', 'Test3', 'Final')

display(tests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a new column based on the subset columns

# COMMAND ----------

tests_df = tests_df.withColumn('TotalTestScore', tests_df.Test1+tests_df.Test2+tests_df.Test3+tests_df.Final)

display(tests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Find the average grade based on HomeState using the new column.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count,avg

homestate_avg_df = tests_df.groupby('HomeState').agg(count('StudentID').alias('TotalStudents'), avg('TotalTestScore').alias('AverageTotalTestScore'))

display(homestate_avg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter the subset based on major and aggregate.

# COMMAND ----------

cs_tests_df = tests_df.filter(tests_df.Major == "Computer Science")
print(cs_tests_df.count())

homestate_cs_avg_df = cs_tests_df.groupby('HomeState').agg(count('StudentID').alias('TotalStudents'), avg('TotalTestScore').alias('AverageTotalTestScore'))

display(homestate_cs_avg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a new column for "MyHomeState" using when statement
# MAGIC
# MAGIC This is useful for creating new columns with basic if/then/else logic.

# COMMAND ----------

from pyspark.sql.functions import when, col

myhome_tests_df = tests_df.withColumn("MyHomeState", when(col('HomeState')=="Minnesota", "Yes").otherwise("No"))

display(myhome_tests_df)

myhome_avg_df = myhome_tests_df.groupby('MyHomeState').agg(count('StudentID').alias('TotalStudents'), avg('TotalTestScore').alias('AverageTotalTestScore'))

display(myhome_avg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read the file with detail on the grade items.  "Melt" the grades dataframe to enable a join with the grade items.
# MAGIC

# COMMAND ----------

# Specify schema on read.
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType

schema = StructType([StructField("Item", StringType(), True), \
                    StructField("Type", StringType(), True), \
                    StructField("TotalPoints", IntegerType(), True), \
                    StructField("Topic", StringType(), True)])
items_df = spark.read.options(delimiter=',', header=True).schema(schema).csv(uri+"Items.csv")

display(items_df)

print(items_df.dtypes)


# COMMAND ----------

# Display the column list that I want to melt, starting with Quiz1 and ending with Final.
print(grades_df.columns[1:len(grades_df.columns)])

# COMMAND ----------

# Use the built in melt function to tranform to long format.
grades_score_long_df = grades_df.melt(ids=['StudentID'], values=grades_df.columns[1:len(grades_df.columns)], variableColumnName='Item', valueColumnName='Score')

display(grades_score_long_df)

# COMMAND ----------

# Filter out items that start with SpecialEvent.  These will be out of scope for this example.
filtered_grades_score_long_df = grades_score_long_df.filter(~grades_score_long_df['Item'].startswith('SpecialEvent'))

display(filtered_grades_score_long_df)

# COMMAND ----------

# Convert Score to an integer
filtered_grades_score_long_df = filtered_grades_score_long_df.withColumn('Score', filtered_grades_score_long_df['Score'].cast('integer'))

display(filtered_grades_score_long_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save the data in long format.
# MAGIC
# MAGIC We'll save in both CSV and Parquet format.
# MAGIC

# COMMAND ----------

filtered_grades_score_long_df.write.option('header',True).mode('overwrite').csv(uri+"Scratch/CSVLong")
filtered_grades_score_long_df.write.format('parquet').mode('overwrite').save(uri+"Scratch/ParquetLong")
filtered_grades_score_long_df.write.format('delta').option('overwriteSchema', 'true').mode('overwrite').save(uri+"Scratch/DeltaLong")

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyze long format.
# MAGIC
# MAGIC Now that we're in long format, we can do a variety of analysis easily.

# COMMAND ----------

# Join the student and item data with the long format.
all_join_df = filtered_grades_score_long_df.join(students_df, on='StudentID', how='inner')

all_join_df = all_join_df.join(items_df, on='Item', how='inner')

display(all_join_df)
                           
                              


# COMMAND ----------

# Calculate total score by student.
from pyspark.sql.functions import sum, desc

student_agg_df = all_join_df.groupby('StudentID', 'Major', 'HomeState').agg(sum('Score').alias('StudentTotalPoints'),
                                                                            sum('TotalPoints').alias('TotalPointsAvailable')).orderBy(desc('StudentTotalPoints'))

display(student_agg_df)

# COMMAND ----------

# Calculate average score by major.
display(student_agg_df.groupby('Major').agg(count('StudentID').alias('StudentCount'), avg('StudentTotalPoints').alias('AverageTotal')))

# COMMAND ----------

# Calculate total score by assignment type.
display(all_join_df.groupby('Type').agg(sum('Score').alias('ItemTotalPoints'),
                                                                            sum('TotalPoints').alias('TotalPointsAvailable')).orderBy(desc('ItemTotalPoints')))


# COMMAND ----------

# Find the students that are in the student list but don't have any grades.
# This is a good job for an anti-join.
students_notinclass_df = students_df.join(all_join_df, on='StudentID', how='anti')

display(students_notinclass_df)
