# # An Introduction to CML Native Workbench

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Setup environment
from env import S3_ROOT, S3_HOME
import os
os.environ["S3_ROOT"] = S3_ROOT
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# ## Entering Code

# Enter code as you normally would in a Python shell or script:

print("Hello, Data Scientists!")

2 + 2

import math
def expit(x):
    """Compute the expit function (aka inverse-logit function or logistic function)."""
    return 1.0 / (1.0 + math.exp(-x))
expit(2)

import seaborn as sns
iris = sns.load_dataset("iris")
sns.pairplot(iris, hue="species")


# ## Getting Help

# Use the standard Python and IPython commands to get help:

help(expit)

expit?

#expit??



# ## Accessing the Linux Command Line

# Run a Linux command by using the `!` prefix.

# Print the current working directory:
!pwd

# **Note:** All CML Native Workbench users have the username `cdsw`.

# List the contents of the current directory:
!ls -l

# List the contents of the `/duocar` directory in the Hadoop Distributed File
# System (HDFS):
!hdfs dfs -ls $S3_ROOT/duocar

# You can also access the command line via the **Terminal access** menu item.


# ## Working with Python Packages

# Use `pip` in Python 2 or `pip3` in Python 3 to manage Python packages.

# **Important:** Packages are managed on a project-by-project basis.

# Get a list of the currently installed packages:
!pip3 list

# Install a new package:
!pip3 install folium

# **Note:** This package is now available in all future sessions launched in
# this project.

# Show details about the installed package:
!pip3 show folium

# **Note:**  This returns nothing if the package is not installed.

# Import the package:
import folium

# Use the package:
folium.Map(location=[46.8772222, -96.7894444])

# Uninstall the package:
#```python
#!pip3 uninstall -y folium
#```

# **Note:** Include the `-y` option to avoid an invisible prompt to confirm the
# uninstall.

# **Important:** This package is no longer available in all future sessions
# launched in this project.


# ## Formatting Session Output

# Use [Markdown](https://daringfireball.net/projects/markdown/syntax) text to
# format your session output.

# **Important:** Prefix the markdown text with the comment character `#`.

# ### Headings

# # Heading 1

# ## Heading 2

# ### Heading 3

# ### Text

# Plain text

# *Emphasized text* or _emphasized text_

# **Bold text** or __bold text__

# `Code text` (Note these are backtick quotes.)

# ### Mathematical Text

# Display an inline term like $\bar{x} = \frac{1}{n} \sum_{i=1}^{n} x_i$ using
# a [LaTeX](https://en.wikibooks.org/wiki/LaTeX/Mathematics) expression
# surrounded by dollar-sign characters.

# A math expression can be displayed set apart by surrounding the LaTeX
# shorthand with double dollar-signs, like so: $$f(x)=\frac{1}{1+e^{-x}}$$

# ### Lists

# Bulleted List
# * Item 1
#   * Item 1a
#   * Item 1b
# * Item 2
# * Item 3

# Numbered List
# 1. Item 1
# 2. Item 2
# 3. Item 3

# ### Links

# Link to [Cloudera](http://www.cloudera.com).

# ### Images

# Display a stored image file:
from IPython.display import Image
Image("exercises/code/resources/spark.png")

# **Note:** The image path is relative to `/home/cdsw/` regardless of script
# location.

# ### Code blocks

# To print a block of code in the output without running it, use a comment line
# with three backticks to begin the block, then the block of code with each
# line preceded with the comment character, then a comment line with three
# backticks to close the block. Optionally include the language name after the
# opening backticks:

#```python
#print("Hello, Data Scientists!")
#```

# You can omit the language name to print the code block in black text without
# syntax coloring, for example, to display a block of static data or output:

#```
#Hello, Data Scientists!
#```

# ### Invisible comments

#[//]: # (To include a comment that will not appear in the)
#[//]: # (output at all, you can use this curious syntax.)

# Move along, nothing to see here.


# ## Exercises

# (1) Experiment with the CML Native Workbench command line.

# * Type a partial command and then press the TAB key.  Use the UP and DOWN
# ARROW keys to navigate the tab completion options.  Press the RETURN key to
# select a tab completion option.

# * Use the LEFT and RIGHT ARROW keys to navigate the command line.

# * Use the UP and DOWN ARROW keys to navigate the command line history.

# * Use SHIFT-RETURN to enter a multi-line command.

# (2) Experiment with the CML Native Workbench file editor.

# * Create a new file called `logit.py`.

# * Enter the following text and code into the file. **Note:** Do not enter
# the lines with three backticks. Delete the first comment character from each
# line.

#```python
## # Define and Evaluate the Logit Function
#
## Define the logit function:
#import numpy as np
#def logit(x): return np.log(x/(1.0-x))
#
## Evaluate the logit function:
#p = np.array([0.1, 0.25, 0.5, 0.75, 0.9])
#logit(p)
#```

# * Select a line or block of code and run the selection.

# * Clear the console log and run the entire file.

# (3) Experiment with the Linux command line.

# * Click on **Terminal access** to open a terminal window.

# * Use the `hdfs dfs -ls` command to explore the `/duocar/raw/` directory in
# HDFS.


# ## References

# [Cloudera Data Science Workbench](https://docs.cloudera.com/documentation/data-science-workbench/latest.html)

# [Markdown](https://www.markdownguide.org)

# [LaTeX](https://en.wikibooks.org/wiki/LaTeX/Mathematics)
# # Running a Spark Application from CML Native Workbench

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to do the following:
# * Start a Spark application
# * Read a comma-delimited file from HDFS into a Spark SQL DataFrame
# * Examine the schema of a Spark SQL DataFrame
# * Calculate the dimensions of a Spark SQL DataFrame
# * Examine some rows of a Spark SQL DataFrame
# * Stop a Spark application 


# ## Running a Spark Application from CML Native Workbench

# * A Spark *application* runs on the Java Virtual Machine (JVM)

# * An application consists of a set of Java processes
#   * A *driver* process coordinates the work
#   * A set of *executor* processes perform the work

# * An application normally runs on the Hadoop cluster via YARN
#   * Used to process, analyze, and model large data sets
#   * The driver runs in the CML Native Workbench session engine
#   * The executors run in the worker nodes

# * An application can run locally within the CML Native Workbench session engine
#   * Used to develop and test code on small data sets
#   * The driver runs in the CML Native Workbench session engine
#   * The executors run as threads in the context of the driver process

# * Start an application by creating an instance of the `SparkSession` class

# * Stop an application by calling the `stop` method of the `SparkSession`
# instance


# ## Starting a Spark Application

# Import the `SparkSession` class from the `pyspark.sql` module:
from pyspark.sql import SparkSession

# Use the cmldata module to create a data connection and spark session
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# Call the optional `master` method to specify how the application runs:
# * Pass `yarn` to run on the Hadoop cluster via YARN
# * Pass `local` to run in the CML Native Workbench session engine with one thread
# * Pass `local[N]` to run in the CML Native Workbench session engine with $N$ threads
# * Pass `local[*]` to run in the CML Native Workbench session engine with all available
# threads

# Call the optional `appName` method to specify a name for the application.

# Call the required `getOrCreate` method to create the instance.

# **Note:** `spark` is the conventional name for the `SparkSession` instance.

# **Note:** The backslash `\` is the Python line continuation character.

# Access the `version` attribute of the `SparkSession` instance to get the
# Spark version number:
spark.version

# **Note:**  We are using the Cloudera Distribution of Apache Spark.


# ## Reading data into a Spark SQL DataFrame

# Use the `csv` method of the `DataFrameReader` class to read the raw ride data
# from HDFS into a DataFrame:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", sep=",", header=True, inferSchema=True)

# **Note:** HDFS is the default file system for Spark in CML Native Workbench.


# ## Examining the schema of a DataFrame

# Call the `printSchema` method to print the schema:
rides.printSchema()

# Access the `columns` attribute to get a list of column names:
rides.columns

# Access the `dtypes` attribute to get a list of column names and data types:
rides.dtypes

# Access the `schema` attribute to get the schema as a instance of the `StructType` class:
rides.schema


# ## Computing the number of rows and columns of a DataFrame

# Call the `count` method to compute the number of rows:
rides.count()

# Pass the list of column names to the Python `len` function to compute the
# number of columns:
len(rides.columns)


# ## Examining a few rows of a DataFrame

# Call the `show` method to print some rows of a DataFrame:
rides.show(5)
rides.show(5, truncate=5)
rides.show(5, vertical=True)

# Call the `head` or `take` method to get a list of `Row` objects from a
# DataFrame:
rides.head(5)
rides.take(5)


# ## Stopping a Spark Application

# Call the `stop` method to stop the application:
spark.stop()

# **Note:** The Spark application will also stop when you stop the CML Native Workbench session
# engine.


# ## Exercises

# (1) Create a new `SparkSession` and configure Spark to run locally with one
# thread.

# (2) Read the raw driver data from HDFS.

# (3) Examine the schema of the drivers DataFrame.

# (4) Count the number of rows of the drivers DataFrame.

# (5) Examine a few rows of the drivers DataFrame.

# (6) **Bonus:** Repeat exercises (2)-(5) with the raw rider data.

# (7) **Bonus:** Repeat exercises (2)-(5) with the raw ride review data.
# **Hint:** Verify the file format before reading the data.

# (8) Stop the SparkSession.


# ## References

# [Apache Spark](https://spark.apache.org/)

# [Spark Documentation](https://spark.apache.org/docs/latest/index.html)

# [Spark Documentation - SQL, DataFrames, and Datasets
# Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

# [Spark Python API - pyspark.sql.SparkSession
# class](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession)

# [Spark Python API - pyspark.sql.DataFrame
# class](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Using CDS 2.x Powered by Apache
# Spark](https://docs.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_dist_comp_with_Spark.html)
# # Running a Spark Application from CML Native Workbench - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Set environment variables
from env import S3_ROOT, S3_HOME, CONNECTION_NAME
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# ## Exercises

# (1) Create a new `SparkSession` and configure Spark to run locally with one
# thread.

# Use the cmldata module to create a data connection and spark session
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# (2) Read the raw driver data from HDFS.

drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)

# (3) Examine the schema of the drivers DataFrame.

drivers.printSchema()

# (4) Count the number of rows of the drivers DataFrame.

drivers.count()

# (5) Examine a few rows of the drivers DataFrame.

drivers.head(5)

# (6) **Bonus:** Repeat exercises (2)-(5) with the raw rider data.

riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)
riders.printSchema()
riders.count()
riders.head(5)

# (7) **Bonus:** Repeat exercises (2)-(5) with the raw ride review data.
# **Hint:** Verify the file format before reading the data.

reviews = spark.read.csv(S3_ROOT + "/duocar/raw/ride_reviews/", sep="\t", header=False, inferSchema=True)
reviews.printSchema()
reviews.count()
reviews.show(5)

# (8) Stop the SparkSession.

spark.stop()
# # Inspecting a Spark DataFrame

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we inspect our DataFrame more carefully.  In particular, we
# inspect columns that represent the following types of variables:
 
# * Primary key variable
# * Categorical variable
# * Continuous numerical variable
# * Date and time variable
 
# In the process, we introduce various Spark SQL functionality that we cover
# more formally in subsequent modules.


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Read the raw ride data from HDFS to a Spark SQL DataFrame

rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)


# ## Inspecting a DataFrame

# Print the schema:
rides.printSchema()

# Use the *pandas* library to display the DataFrame as a scrollable HTML table:
import pandas as pd
pd.options.display.html.table_schema=True
rides.limit(5).toPandas()

# **Note:** The `limit` method returns a DataFrame with the specified number of
# rows; the `toPandas` method returns a pandas DataFrame.

# Use the `describe` method to get basic summary statistics on each column:
rides.describe().toPandas()

# **Note:**  The `describe` method returns a DataFrame.


# ## Inspecting a DataFrame column

# ### Inspecting a primary key variable

# The `id` column represents a primary key variable:
rides.select("id").show(10)

# **Note:** `select` is a DataFrame method that returns a DataFrame.

# The `id` column should be non-null and unique.  Count the number of missing
# (null) values:
rides.filter(rides.id.isNull()).count()

# **Note:** `filter` is a DataFrame method that returns a DataFrame consisting
# of the rows for which its argument is true.  `rides.id` is a Column object
# and `isNull` is a Column method.

# Count the number of rows:
rides.count()

# Count the number of distinct values:
rides.select("id").distinct().count()

# Count the number of non-missing and distinct values using Column functions:
from pyspark.sql.functions import count, countDistinct
rides.select(count("*"), count("id"), countDistinct("id")).show()

# We have been using the DataFrame API of Spark SQL.  To use the SQL API to
# count the number of non-missing and distinct values, first register the
# DataFrame as a *temporary view*:
rides.createOrReplaceTempView("rides_view")

# Then use the `sql` method to run a query:
spark.sql("SELECT COUNT(*), COUNT(id), COUNT(DISTINCT id) FROM rides_view").show()

# **Note:** The `sql` method returns a DataFrame.


# ### Inspecting a categorical variable

# The `service` column represents a categorical variable:
rides.select("service").show(10)

# **Question:** What do the missing (null) values represent?

# Count the number of missing (null) values:
rides.filter(rides.service.isNull()).count()

# Count the number of distinct values:
rides.select("service").distinct().count()

# Print the distinct values:
rides.select("service").distinct().show()

# Count the number of rides by service:
rides.groupby("service").count().show()

# Use the SQL API to count the number of rides by service:
spark.sql("SELECT service, COUNT(*) FROM rides_view GROUP BY service").show()
  
# Use pandas to plot the number of rides by service:
rides.groupby("service").count().toPandas().plot(x="service", y="count", kind="bar")


# ### Inspecting a numerical variable

# The `distance` column represents a numerical variable (stored as an
# integer):
rides.select("distance").show(10)

# Use the `describe` method to compute basic summary statistics:
rides.describe("distance").show()

# **Question:** Are there any missing (null) values?

# Use the `approxQuantile` method to get customized quantiles:
rides.approxQuantile("distance", \
	probabilities=[0.0, 0.25, 0.5, 0.75, 1.0], \
	relativeError=1e-5)

# **Note:** The `approxQuantile` method returns a Python list.

# **Question:** Why does Spark produce approximate quantiles?

# See the documentation for more details:
rides.approxQuantile?

# Use pandas to plot a basic histogram:
rides.select("distance").toPandas().plot(kind="hist")

# **Warning:** `toPandas()` is dangerous in the Spark world.  Why?
 

# ### Inspecting a date and time variable

# The `date_time` column represents a date and time variable:
rides.select("date_time").show(10)

# However, Spark read it in as a string:
rides.select("date_time").printSchema()

# Use the `cast` method to convert it to a timestamp:
dates = rides.select("date_time", rides.date_time.cast("timestamp").alias("date_time_fixed"))
dates.show(5)

# Note that timestamps are represented by Python `datetime` objects:
dates.head(5)

# Note that the `describe` method does not generate summary statistics for date
# and time variables (unless they are represented as strings):
dates.describe().show(5)


# ## Exercises

# (1) Read the raw driver data into a Spark DataFrame called `drivers`.

# (2) Examine the inferred schema.  Do the data types seem appropriate?

# (3) Verify the integrity of the putative primary key `id`.

# (4) Inspect `birth_date`.  What data type did Spark infer?

# (5) Determine the unique values of `student`.  What type of variable do you
# think `student` is?

# (6) Count the number of drivers by `vehicle_make`.  What is the most popular
# make?

# (7) Compute basic summary statistics on the `rides` column.  How does the
# mean number of rides compare to the median?

# (8) **Bonus:** Inspect additional columns of the `drivers` DataFrame.

# (9) **Bonus:** Inspect the raw rider data.


# ## References

# [Spark Python API - pyspark.sql.DataFrame
# class](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Spark Python API - pyspark.sql.Column
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)

# [Spark Python API - pyspark.sql.types
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)

# [pandas Documentation -
# Visualization](http://pandas.pydata.org/pandas-docs/stable/visualization.html)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Inspecting a Spark DataFrame - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"


# ## Exercises

# (1) Read the raw driver data into a Spark DataFrame called `drivers`.

drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)

# (2) Examine the inferred schema.  Do the data types seem appropriate?

drivers.printSchema()

# (3) Verify the integrity of the putative primary key `id`.

from pyspark.sql.functions import count, countDistinct
drivers.select(count("*"), count("id"), countDistinct("id")).show()

# (4) Inspect `birth_date`.  What data type did Spark infer?

drivers.select("birth_date").show(5)

# Spark inferred the data type to be a date AND time even though the column
# represents a pure date.  This is likely to ensure compatibility with Apache
# Impala, which does not support pure dates at this time.

# (5) Determine the unique values of `student`.  What type of variable do you
# think `student` is?

drivers.select("student").distinct().show()

# (6) Count the number of drivers by `vehicle_make`.  What is the most popular
# make?

drivers.groupBy("vehicle_make").count().show(n=30)

# (7) Compute basic summary statistics on the `rides` column.  How does the
# mean number of rides compare to the median?

drivers.describe("rides").show()
drivers.approxQuantile("rides", probabilities=[0.0, 0.25, 0.5, 0.75, 1.0], relativeError=1e-5)

# (8) **Bonus:** Inspect additional columns of the `drivers` DataFrame.

# (9) **Bonus:** Inspect the raw rider data.


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Transforming DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate some basic transformations on DataFrames:
# * Working with columns
#   * Selecting columns
#   * Dropping columns
#   * Specifying columns
#   * Adding columns
#   * Changing the column name
#   * Changing the column type
# * Working with rows
#   * Ordering rows
#   * Keeping a fixed number of rows
#   * Keeping distinct rows
#   * Filtering rows
#   * Sampling rows
# * Working with missing values

# **Note:** There is often more than one way to do these transformations.


# ## Spark SQL DataFrames

# * Spark SQL DataFrames are inspired by R data frames and Python pandas DataFrames

# * Spark SQL DataFrames are resilient distributed structured tables
#   * A DataFrame is a distributed collection of *Row* objects
#   * A Row object is an ordered collection of objects with names and types

# * Properties of DataFrames
#   * Immutable - DataFrames are read-only
#   * Evaluated lazily - Spark only does work when it has to
#   * Ephemeral - DataFrames disappear unless explicitly persisted

# * Operations on DataFrames
#   * *Transformations* return a DataFrame
#     * Narrow Transformations
#     * Wide Transformations
#   * *Actions* cause Spark to do work

# * Spark SQL provides two APIs
#   * SQL
#   * DataFrame

# * Spark SQL code is declarative - the Catalyst optimizer produces core Spark RDD code


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw data from HDFS:

rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
rides.printSchema()

drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
drivers.printSchema()

riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)
riders.printSchema()


# ## Working with Columns

# ### Selecting Columns

# Use the `select` method to select specific columns:
riders.select("birth_date", "student", "sex").printSchema()

# ### Dropping Columns

# Use the `drop` method to drop specific columns:
riders.drop("first_name", "last_name", "ethnicity").printSchema()

# ### Specifying Columns

# We have used the column name to reference a DataFrame column:
riders.select("first_name").printSchema()

# We actually work with `Column` objects in Spark SQL.
# Use the following syntax to reference a column object in a particular DataFrame:
riders.select(riders.first_name).printSchema()
riders.select(riders["first_name"]).printSchema()
type(riders["first_name"])

# Use the `col` or `column` function to reference a general column object:
from pyspark.sql.functions import col, column
riders.select(col("first_name")).printSchema()
riders.select(column("first_name")).printSchema()
type(column("first_name"))

# Use `*` (in quotes) to specify all columns:
riders.select("*").printSchema()

# ### Adding Columns

# Use the `withColumn` method to add a new column:
riders \
  .select("student") \
  .withColumn("student_boolean", col("student") == 1) \
  .show()

# The `select` method also works:
riders.select("student", (col("student") == 1).alias("student_boolean")).show(5)

# The `selectExpr` method accepts partial SQL expressions:
riders.selectExpr("student", "student = 1 as student_boolean").show(5)

# The `sql` method accepts full SQL statements:
riders.createOrReplaceTempView("riders_view")
spark.sql("select student, student = 1 as student_boolean from riders_view").show(5)

# ### Changing the column name

# Use the `withColumnRenamed` method to rename a column:
riders.withColumnRenamed("start_date", "join_date").printSchema()

# Chain multiple methods to rename more than one column:
riders \
  .withColumnRenamed("start_date", "join_date") \
  .withColumnRenamed("sex", "gender") \
  .printSchema()

# ### Changing the column type

# Recall that `home_block` was read in as a (long) integer:
riders.printSchema()

# Use the `withColumn` (DataFrame) method in conjunction with the `cast`
# (Column) method to change its type:
riders.withColumn("home_block", col("home_block").cast("string")).printSchema()

# **Note:** If we need to change the name and/or type of many columns, then we
# may want to consider specifying the schema on read.


# ## Working with rows

# ### Ordering rows

# Use the `sort` or `orderBy` method to sort a DataFrame by particular columns:
rides \
  .select("rider_id", "date_time") \
  .sort("rider_id", "date_time", ascending=True) \
  .show()

# **Note:** Ascending order is the default.

rides \
  .select("rider_id", "date_time") \
  .orderBy("rider_id", "date_time", ascending=False) \
  .show()

# Use the `asc` and `desc` methods to specify the sort order:
rides \
  .select("rider_id", "date_time") \
  .sort(col("rider_id").asc(), col("date_time").desc()) \
  .show()

# Alternatively, use the `asc` and `desc` functions to specify the sort order:
from pyspark.sql.functions import asc, desc
rides \
  .select("rider_id", "date_time") \
  .orderBy(asc("rider_id"), desc("date_time")) \
  .show()

# ### Selecting a fixed number of rows

# Use the `limit` method to select a fixed number of rows:
riders.select("student", "sex").limit(5).show()

# **Question:** What is the difference between `df.show(5)` and `df.limit(5).show()`?

# ### Selecting distinct rows

# Use the `distinct` method to select distinct rows:
riders.select("student", "sex").distinct().show()

# You can also use the `dropDuplicates` method:
riders.select("student", "sex").dropDuplicates().show()

# ### Filtering rows

# Use the `filter` or `where` method along with a Boolean column expression to select
# particular rows:
riders.filter(col("student") == 1).count()
riders.where(col("sex") == "female").count()
riders.filter(col("student") == 1).where(col("sex") == "female").count()

# ### Sampling rows

# Use the `sample` method to select a random sample of rows with or without
# replacement:
riders.count()
riders.sample(withReplacement=False, fraction=0.1, seed=12345).count()

# Use the `sampleBy` method to select a stratified random sample:
riders \
  .groupBy("sex") \
  .count() \
  .show() 
riders \
  .sampleBy("sex", fractions={"male": 0.2, "female": 0.8}, seed=54321) \
  .groupBy("sex") \
  .count() \
  .show()

# We have randomly sampled 20% of the male riders and 80% of the female riders.


# ## Working with missing values

# Note the missing (null) values in the following DataFrame:
riders_selected = riders.select("id", "sex", "ethnicity")
riders_selected.show(25)

# Drop rows with any missing values:
riders_selected.dropna(how="any", subset=["sex", "ethnicity"]).show(25)

# Drop rows with all missing values:
riders_selected.na.drop(how="all", subset=["sex", "ethnicity"]).show(25)

# **Note**: `dropna` and `na.drop` are equivalent.

# Replace missing values with a common value:
riders_selected.fillna("OTHER/UNKNOWN", ["sex", "ethnicity"]).show(25)

# Replace missing values with different values:
riders_missing = riders_selected.na.fill({"sex": "OTHER/UNKNOWN", "ethnicity": "MISSING"})
riders_missing.show(25)

# **Note**: `fillna` and `na.fill` are equivalent.

# **Note**: The fill value type must match the column type.

# Replace arbitrary values with a common value:
riders_missing.replace(["OTHER/UNKNOWN", "MISSING"], "NA", ["sex", "ethnicity"]).show(25)

# Replace arbitrary values with different values:
riders_missing.na.replace({"OTHER/UNKNOWN": "NA", "MISSING": "NO RESPONSE"}, None, ["sex", "ethnicity"]).show(25)

# **Note:** `replace` and `na.replace` are equivalent.


# ## Exercises

# (1) Replace the missing values in `rides.service` with the string `Car`.

# (2) Rename `rides.cancelled` to `rides.canceled`.

# (3) Sort the `rides` DataFrame in descending order with respect to
# `driver_id` and ascending order with respect to `date_time`.

# (4) Create an approximate 20% random sample of the `rides` DataFrame.

# (5) Remove the driver's name from the `drivers` DataFrame.

# (6) How many drivers have signed up?  How many female drivers have signed up?
# How many non-white, female drivers have signed up?


# ## References

# [Spark Python API - pyspark.sql.DataFrame
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Spark Python API - pyspark.sql.DataFrameNaFunctions
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameNaFunctions)

# [Spark Python API - pyspark.sql.Column
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Transforming DataFrames - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# Read the raw data from HDFS:

rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)


# ## Exercises

# (1) Replace the missing values in `rides.service` with the string `Car`.

rides.select("service").distinct().show()
rides_filled = rides.fillna("Car", subset=["service"])
rides_filled.select("service").distinct().show()

# (2) Rename `rides.cancelled` to `rides.canceled`.

rides_renamed = rides.withColumnRenamed("cancelled", "canceled")
rides_renamed.printSchema()

# (3) Sort the `rides` DataFrame in descending order with respect to
# `driver_id` and ascending order with respect to `date_time`.

rides_sorted = rides.sort(rides.driver_id.desc(), "date_time")
rides_sorted.select("driver_id", "date_time").show()

# (4) Create an approximate 20% random sample of the `rides` DataFrame.

rides.count()
rides_sampled = rides.sample(withReplacement=False, fraction=0.2, seed=31416)
rides_sampled.count()

# (5) Remove the driver's name from the `drivers` DataFrame.

drivers_fixed = drivers.drop("first_name", "last_name")
drivers_fixed.printSchema()

# (6) How many drivers have signed up?  How many female drivers have signed up?
# How many non-white, female drivers have signed up?

drivers.count()
drivers.filter(drivers.sex == "female").count()
drivers.filter(drivers.ethnicity != "White").filter(drivers.sex == "female").count()
drivers.filter(((drivers.ethnicity != "White") | (drivers.ethnicity.isNull())) & (drivers.sex == "female")).count()


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Transforming DataFrame Columns

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# Spark SQL supports various column types and provides a variety of functions
# and Column methods that can be applied to each type.  In this module we
# demonstrate how to transform DataFrame columns of various types.

# * Working with numerical columns
# * Working with string columns
# * Working with datetime columns
# * Working with Boolean columns


# ## Spark SQL Data Types

# * Spark SQL data types are defined in the `pyspark.sql.types` module

# * Spark SQL supports the following basic data types:
#   * NullType
#   * StringType
#   * Byte array data type
#     * BinaryType
#   * BooleanType
#   * Integer data types
#     * ByteType
#     * ShortType
#     * IntegerType
#     * LongType
#   * Fixed-point data type
#     * DecimalType
#   * Floating-point data types
#     * FloatType
#     * DoubleType
#   * Date and time data types
#     * DateType
#     * TimestampType

# * Spark also supports the following complex (collection) types:
#   * ArrayType
#   * MapType
#   * StructType

# * Spark SQL provides various methods and functions that can be applied to the
# various data types


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)


# ## Working with numerical columns

# ### Example 1: Converting ride distance from meters to miles

from pyspark.sql.functions import col, round
rides \
  .select("distance", round(col("distance") / 1609.344, 2).alias("distance_in_miles")) \
  .show(5)

# **Notes:**
# * We have used the fact that 1 mile = 1609.344 meters.
# * We have used the `round` function to round the result to two decimal places.
# * We have used the `alias` method to rename the column.

# To add a new column, use the `withColumn` method with a new column name:
rides \
  .withColumn("distance_in_miles", round(col("distance") / 1609.344, 2)) \
  .printSchema()

# To replace an existing column, use the `withColumn` method with an existing
# column name:
rides \
  .withColumn("distance", round(col("distance") / 1609.344, 2)) \
  .printSchema()

# ### Example 2: Converting the ride id from an integer to a string

# Use the `format_string` function to convert `id` to a left-zero-padded string:
from pyspark.sql.functions import format_string
rides \
  .withColumn("id_fixed", format_string("%010d", "id")) \
  .select("id", "id_fixed") \
  .show(5)

# **Note:** We have used the [printf format
# string](https://en.wikipedia.org/wiki/Printf_format_string) `%010d` to
# achieve the desired format.

# ### Example 3: Converting the student flag from an integer to a Boolean

# Using a Boolean expression:
riders \
  .withColumn("student_boolean", col("student") == 1) \
  .select("student", "student_boolean") \
  .show(5)

# Using the `cast` method:
riders \
  .withColumn("student_boolean", col("student").cast("boolean")) \
  .select("student", "student_boolean") \
  .show(5)


# ## Working with string columns

# ### Example 4: Normalizing a string column

# Use the `trim` and `upper` functions to normalize `riders.sex`:
from pyspark.sql.functions import trim, upper
riders \
  .withColumn("gender", upper(trim(col("sex")))) \
  .select("sex", "gender") \
  .show(5)

# ### Example 5: Extracting a substring from a string column

# The [Census Block Group](https://en.wikipedia.org/wiki/Census_block_group) is
# the first 12 digits of the [Census
# Block](https://en.wikipedia.org/wiki/Census_block).  Use the `substring`
# function to extract the Census Block Group from `riders.home_block`:
from pyspark.sql.functions import substring
riders \
  .withColumn("home_block_group", substring("home_block", 1, 12)) \
  .select("home_block", "home_block_group") \
  .show(5)

# ### Example 6: Extracting a substring using a regular expression

# Use the `regexp_extract` function to extract the Census Block Group via a
# regular expression:
from pyspark.sql.functions import regexp_extract
riders \
  .withColumn("home_block_group", regexp_extract("home_block", "^(\d{12}).*", 1)) \
  .select("home_block", "home_block_group") \
  .show(5)

# **Note:** The third argument to `regexp_extract` is the capture group.


# ## Working with date and timestamp columns

# ### Example 7: Converting a timestamp to a date 

# Note that `riders.birth_date` and `riders.start_date` were read in as timestamps:
riders.select("birth_date", "start_date").show(5)

# Use the `cast` method to convert `riders.birth_date` to a date:
riders \
  .withColumn("birth_date_fixed", col("birth_date").cast("date")) \
  .select("birth_date", "birth_date_fixed") \
  .show(5)

# Alternatively, use the `to_date` function:
from pyspark.sql.functions import to_date
riders \
  .withColumn("birth_date_fixed", to_date("birth_date")) \
  .select("birth_date", "birth_date_fixed") \
  .show(5)

# ### Example 8: Converting a string to a timestamp

# Note that `rides.date_time` was read in as a string:
rides.printSchema()

# Use the `cast` method to convert it to a timestamp:
rides \
  .withColumn("date_time_fixed", col("date_time").cast("timestamp")) \
  .select("date_time", "date_time_fixed") \
  .show(5)

# Alternatively, use the `to_timestamp` function:
from pyspark.sql.functions import to_timestamp
rides \
  .withColumn("date_time_fixed", to_timestamp("date_time", format="yyyy-MM-dd HH:mm")) \
  .select("date_time", "date_time_fixed") \
  .show(5)

# ### Example 9: Computing the age of each rider

# Use the `current_date` and `months_between` functions to compute the age of
# each rider:
from pyspark.sql.functions import current_date, months_between, floor
riders \
  .withColumn("today", current_date()) \
  .withColumn("age", floor(months_between("today", "birth_date") / 12)) \
  .select("birth_date", "today", "age") \
  .show(5)

# **Note:** Spark implicitly casts `birth_date` or `today` as necessary.  It is
# probably safer to explicitly cast one of these columns before computing the
# number of months between.


# ## Working with Boolean columns

# ### Example 10: Predefining a Boolean column expression

# You can predefine a Boolean column expression:
studentFilter = col("student") == 1
type(studentFilter)

# You can use the predefined expression to create a new column:
riders \
  .withColumn("student_boolean", studentFilter) \
  .select("student", "student_boolean") \
  .show(5)

# Or filter a DataFrame:
riders \
  .filter(studentFilter) \
  .select("student") \
  .show(5)

# ### Example 11: Working with multiple Boolean column expressions

# Predefine the Boolean column expressions:
studentFilter = col("student") == 1
maleFilter = col("sex") == "male"

# Create a new column using the AND (`&`) operator:
riders.select("student", "sex", studentFilter & maleFilter).show(15)

# Create a new column using the OR (`|`) operator:
riders.select("student", "sex", studentFilter | maleFilter).show(15)

# **Important:** The Boolean column expression parser in Spark SQL is not very
# advanced.  Use parentheses liberally in your expressions.

# Note the difference in how nulls are treated in the computation:
# * true & null = null
# * false & null = false
# * true | null = true
# * false | null = null

# ### Example 12: Using multiple Boolean expressions in a filter

# Use `&` for a logical AND:
riders.filter(maleFilter & studentFilter).select("student", "sex").show(5)

# This is equivalent to
riders.filter(maleFilter).filter(studentFilter).select("student", "sex").show(5)

# Use `|` for a logical OR:
riders.filter(maleFilter | studentFilter).select("student", "sex").show(5)

# Be careful with missing (null) values:
riders.select("sex").distinct().show()
riders.filter(col("sex") != "male").select("sex").distinct().show()


# ## Exercises

# (1) Extract the hour of day and day of week from `rides.date_time`.

# (2) Convert `rides.duration` from seconds to minutes.

# (3) Convert `rides.cancelled` to a Boolean column.

# (4) Create an integer column named `five_star_rating` that is 1.0 if the ride
# received a five-star rating and 0.0 otherwise.

# (5) Create a new column containing the full name for each driver.

# (6) Create a new column containing the average star rating for each driver.

# (7) Find the rider names that are most similar to `Brian`.  **Hint:** Use the
# [Levenshtein](https://en.wikipedia.org/wiki/Levenshtein_distance) function.


# ## References

# [Spark Python API - pyspark.sql.types
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)

# [Spark Python API - pyspark.sql.DataFrame
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Spark Python API - pyspark.sql.Column
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Transforming DataFrame Columns - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# Read the raw data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)


# ## Exercises

# (1) Extract the hour of day and day of week from `rides.date_time`.

from pyspark.sql.functions import hour, dayofweek
rides \
  .withColumn("hour_of_day", hour("date_time")) \
  .withColumn("day_of_week", dayofweek("date_time")) \
  .select("date_time", "hour_of_day", "day_of_week") \
  .show(5)

# (2) Convert `rides.duration` from seconds to minutes.

from pyspark.sql.functions import col, round
rides \
  .withColumn("duration_in_minutes", round(col("duration") / 60, 1)) \
  .select("duration", "duration_in_minutes") \
  .show(5)

# (3) Convert `rides.cancelled` to a Boolean column.

# Using the `cast` method:
rides \
  .withColumn("cancelled", col("cancelled").cast("boolean")) \
  .select("cancelled") \
  .show(5)

# Using a Boolean expression:
rides \
  .withColumn("cancelled", col("cancelled") == 1) \
  .select("cancelled") \
  .show(5)

# (4) Create an integer column named `five_star_rating` that is 1.0 if the ride
# received a five-star rating and 0.0 otherwise.

# Using a Boolean expression and the `cast` method:
rides \
  .withColumn("five_star_rating", (col("star_rating") > 4.5).cast("double")) \
  .select("star_rating", "five_star_rating") \
  .show(10)

# Using the `when` function and the `when` and `otherwise` methods:
from pyspark.sql.functions import when
rides \
  .withColumn("five_star_rating", when(col("star_rating").isNull(), None).when(col("star_rating") == 5, 1.0).otherwise(0.0)) \
  .select("star_rating", "five_star_rating") \
  .show(10)

# **Note:** Beware of null values when generating new columns.

# (5) Create a new column containing the full name for each driver.

from pyspark.sql.functions import concat_ws
drivers \
  .withColumn("full_name", concat_ws(" ", "first_name", "last_name")) \
  .select("first_name", "last_name", "full_name") \
  .show(5)

# (6) Create a new column containing the average star rating for each driver.

drivers \
  .withColumn("star_rating", round(col("stars") / col("rides"), 2)) \
  .select("rides", "stars", "star_rating") \
  .show(5)

# (7) Find the rider names that are most similar to `Brian`.  **Hint:** Use the
# [Levenshtein](https://en.wikipedia.org/wiki/Levenshtein_distance) function.

from pyspark.sql.functions import lit, levenshtein
riders \
  .select("first_name") \
  .distinct() \
  .withColumn("distance", levenshtein(col("first_name"), lit("Brian"))) \
  .sort("distance") \
  .show()


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Complex Types

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we consider the complex collection data types provided by
# Spark SQL: arrays, maps, and structs.  We show how to construct and
# transform columns with complex collection data types.


# ## Complex Collection Data Types

# * Complex collection data types provide a way to renormalize the denormalized
# data that is common in the big-data world

# * The complex collection data types are defined in the `pyspark.sql.types`
# module:
#   * The *ArrayType* represents a variable-length collection of elements
#   * The *MapType* represents a variable-length collection of key-value pairs
#   * The *StructType* represents a fixed-length collection of named elements

# * Complex collection data types are obtained in several ways:
#   * Inherited from Hive/Impala tables
#   * Inferred from nested JSON files
#   * Generated by aggregate functions such as `collect_list` and `collect_set`
#   * Constructed manually


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)


# ## Arrays

# Use the
# [array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.array)
# function to create an array from multiple columns:
from pyspark.sql.functions import array
drivers_array = drivers \
  .withColumn("vehicle_array", array("vehicle_make", "vehicle_model")) \
  .select("vehicle_make", "vehicle_model", "vehicle_array")
drivers_array.printSchema()
drivers_array.show(5, False)

# Use index notation to access elements of the array:
from pyspark.sql.functions import col
drivers_array \
  .select("vehicle_array", col("vehicle_array")[0]) \
  .show(5, False)

# Use the
# [size](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.size)
# function to get the length of the array:
from pyspark.sql.functions import size
drivers_array \
  .select("vehicle_array", size("vehicle_array")) \
  .show(5, False)

# Use the
# [sort_array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.sort_array)
# function to sort the array:
from pyspark.sql.functions import sort_array
drivers_array \
  .select("vehicle_array", sort_array("vehicle_array", asc=True)) \
  .show(5, False)

# Use the `array_contains` function to search the array:
from pyspark.sql.functions import array_contains
drivers_array \
  .select("vehicle_array", array_contains("vehicle_array", "Subaru")) \
  .show(5, False)

# Use the `explode` and `posexplode` functions to explode the array:
from pyspark.sql.functions import explode, posexplode
drivers_array \
  .select("vehicle_array", explode("vehicle_array")) \
  .show(5, False)
drivers_array \
  .select("vehicle_array", posexplode("vehicle_array")) \
  .show(5, False)

# Note that you can pass multiple names to the `alias` method:
drivers_array \
  .select("vehicle_array", posexplode("vehicle_array").alias("position", "column")) \
  .show(5, False)


# ## Maps

# Use the
# [create_map](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.create_map)
# function to create a map:
from pyspark.sql.functions import lit, create_map
drivers_map = drivers \
  .withColumn("vehicle_map", create_map(lit("make"), "vehicle_make", lit("model"), "vehicle_model")) \
  .select("vehicle_make", "vehicle_model", "vehicle_map")
drivers_map.printSchema()
drivers_map.show(5, False)

# Use dot notation to access a value by key:
drivers_map.select("vehicle_map", col("vehicle_map").make).show(5, False)

# Use the `size` function to get the length of the map:
drivers_map.select("vehicle_map", size("vehicle_map")).show(5, False)

# Use the `explode` and `posexplode` functions to explode the map:
drivers_map.select("vehicle_map", explode("vehicle_map")).show(5, False)
drivers_map.select("vehicle_map", posexplode("vehicle_map")).show(5, False)


# ## Structs

# Use the
# [struct](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.struct)
# function to create a struct:
from pyspark.sql.functions import struct
drivers_struct = drivers \
  .withColumn("vehicle_struct", struct(col("vehicle_make").alias("make"), col("vehicle_model").alias("model"))) \
  .select("vehicle_make", "vehicle_model", "vehicle_struct")
drivers_struct.printSchema()
drivers_struct.show(5, False)

# **Note:** The struct is a `Row` object (embedded in a `Row` object).
drivers_struct.head(5)

# Use dot notation to access struct elements:
drivers_struct \
  .select("vehicle_struct", col("vehicle_struct").make) \
  .show(5, False)

# Use the `to_json` function to convert the struct to a JSON string:
from pyspark.sql.functions import to_json
drivers_struct \
  .select("vehicle_struct", to_json("vehicle_struct")) \
  .show(5, False)


# ## Exercises

# (1) Create an array called `home_array` that includes the driver's home
# latitude and longitude.

# (2) Create a map called `name_map` that includes the driver's first and last
# name.

# (3) Create a struct called `name_struct` that includes the driver's first
# and last name.


# ## References

# [Spark Python API -
# pyspark.sql.functions.array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.array)

# [Spark Python API -
# pyspark.sql.functions.create_map](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.create_map)

# [Spark Python API -
# pyspark.sql.functions.struct](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.struct)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Complex Types - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# Read the raw data from HDFS:
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)


# ## Exercises

# (1) Create an array called `home_array` that includes the driver's home
# latitude and longitude.

from pyspark.sql.functions import array

drivers_array = drivers \
  .withColumn("home_array", array("home_lat", "home_lon"))

drivers_array \
  .select("home_lat", "home_lon", "home_array") \
  .show(5, False)

# (2) Create a map called `name_map` that includes the driver's first and last
# name.

from pyspark.sql.functions import lit, create_map

drivers_map = drivers \
  .withColumn("name_map", create_map(lit("first"), "first_name", lit("last"), "last_name"))
  
drivers_map \
  .select("first_name", "last_name", "name_map") \
  .show(5, False)

# (3) Create a struct called `name_struct` that includes the driver's first
# and last name.

from pyspark.sql.functions import col, struct
drivers_struct = drivers \
  .withColumn("name_struct", struct(col("first_name").alias("first"), col("last_name").alias("last")))

from pyspark.sql.functions import to_json
drivers_struct \
  .select("first_name", "last_name", "name_struct", to_json("name_struct")) \
  .show(5, False)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # User-Defined Functions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to create and apply user-defined functions.


# ## User-Defined Functions

# * It is relatively easy to create and apply a user-defined function (UDF)
#   * Define a Python function that operates on a row of data
#   * Register the Python function as a UDF and specify the return type
#   * Apply the UDF as if it were a built-in function

# * Python and any required packages must be installed on the worker nodes
#   * It is possible to distribute required packages via Spark

# * Built-in functions are more efficient than user-defined functions
#   * Use built-in functions when available
#   * Create user-defined functions only when necessary

# * User-defined function are inefficient because of the following:
#   * A Python process must be started alongside each executor
#   * Data must be converted between Java and Python types
#   * Data must be transferred between the Java and Python processes

# * To improve the performance of a UDF:
#   * Use the [Apache Arrow](https://arrow.apache.org/) platform 
#   * Use a vectorized UDF (see the `pandas_udf` function)
#   * Rewrite the UDF in Scala or Java


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw ride data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)

# Cast `date_time` to a timestamp:
from pyspark.sql.functions import col
rides_clean = rides.withColumn("date_time", col("date_time").cast("timestamp"))


# ## Example 1: Hour of Day

# Define the Python function:
import datetime
def hour_of_day(timestamp):
  return timestamp.hour

# **Note:** The Spark `TimestampType` corresponds to Python `datetime.datetime`
# objects.

# Test the Python function:
dt = datetime.datetime(2017, 7, 21, 5, 51, 10)
hour_of_day(dt)

# Register the Python function as a UDF:
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
hour_of_day_udf = udf(hour_of_day, returnType=IntegerType())

# **Note:** We must explicitly specify the return type otherwise it defaults
# to `StringType`.

# Apply the UDF:
rides_clean \
  .select("date_time", hour_of_day_udf("date_time")) \
  .show(5, truncate=False)

# Use the UDF to compute the number of rides by hour of day:
rides_clean \
  .select(hour_of_day_udf("date_time").alias("hour_of_day")) \
  .groupBy("hour_of_day") \
  .count() \
  .orderBy("hour_of_day") \
  .show(25)


# ## Example 2: Great-Circle Distance

# The [great-circle
# distance](https://en.wikipedia.org/wiki/Great-circle_distance) is the
# shortest distance between two points on the surface of a sphere.  In this
# example we create a user-defined function to compute the [haversine
# approximation](https://en.wikipedia.org/wiki/Haversine_formula) to
# the great-circle distance between the ride origin and destination.

# Define the haversine function (based on the code at
# [rosettacode.org](http://rosettacode.org/wiki/Haversine_formula#Python)):
from math import radians, sin, cos, sqrt, asin
def haversine(lat1, lon1, lat2, lon2):
  """
  Return the haversine approximation to the great-circle distance between two
  points (in meters).
  """
  R = 6372.8 # Earth radius in kilometers
 
  dLat = radians(lat2 - lat1)
  dLon = radians(lon2 - lon1)

  lat1 = radians(lat1)
  lat2 = radians(lat2)
 
  a = sin(dLat / 2.0)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2.0)**2
  c = 2.0 * asin(sqrt(a))
 
  return R * c * 1000.0

# **Note:** We have made some minor changes to the code to make it integer
# proof.

# Test the Python function:
haversine(36.12, -86.67, 33.94, -118.40)  # = 2887259.9506071107:

# Register the Python function as a UDF:
from pyspark.sql.types import DoubleType
haversine_udf = udf(haversine, returnType=DoubleType())

# Apply the haversine UDF:
distances = rides \
  .withColumn("haversine_approximation", haversine_udf("origin_lat", "origin_lon", "dest_lat", "dest_lon")) \
  .select("distance", "haversine_approximation")
distances.show(5)

# We expect the haversine approximation to be less than the ride distance:
distances \
  .select((col("haversine_approximation") > col("distance")).alias("haversine > distance")) \
  .groupBy("haversine > distance") \
  .count() \
  .show()

# The null values correspond to cancelled rides:
rides.filter(col("cancelled") == 1).count()

# The true values reflect the fact that the haversine formula is only an
# approximation to the great-circle distance:
distances.filter(col("haversine_approximation") > col("distance")).show(5)


# ## Exercises

# (1) Create a UDF that extracts the day of the week from a timestamp column.
# **Hint:** Use the
# [weekday](https://docs.python.org/2/library/datetime.html#datetime.datetime.weekday)
# method of the Python `datetime` class.  

# (2) Use the UDF to compute the number of rides by day of week.

# (3) Use the built-in function `dayofweek` to compute the number of rides by day of week.


# ## References

# [Python API - datetime
# module](https://docs.python.org/2/library/datetime.html)

# [Spark Python API -
# pyspark.sql.functions.udf](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf)

# [Spark Python API -
# pyspark.sql.functions.pandas_udf](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

# [Cloudera Engineering Blog - Working with UDFs in Apache
# Spark](https://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/)

# [Cloudera Engineering Blog - Use your favorite Python library on PySpark
# cluster with Cloudera Data Science
# Workbench](https://blog.cloudera.com/blog/2017/04/use-your-favorite-python-library-on-pyspark-cluster-with-cloudera-data-science-workbench/)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # User-Defined Functions - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw ride data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)

# Cast `date_time` to a timestamp:
from pyspark.sql.functions import col
rides_clean = rides.withColumn("date_time", col("date_time").cast("timestamp"))


# ## Exercises

# (1) Create a UDF that extracts the day of the week from a timestamp column.
# **Hint:** Use the
# [weekday](https://docs.python.org/2/library/datetime.html#datetime.datetime.weekday)
# method of the Python `datetime` class.  

# Define the Python function:
import datetime
def day_of_week(timestamp):
  return timestamp.weekday()

# Register the Python function as a UDF: 
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
day_of_week_udf = udf(day_of_week, returnType=IntegerType())

# Apply the UDF:
rides_clean \
  .select("date_time", day_of_week_udf("date_time")) \
  .show(5, truncate=False)


# (2) Use the UDF to compute the number of rides by day of week.

rides_clean \
  .withColumn("day_of_week", day_of_week_udf("date_time")) \
  .groupBy("day_of_week") \
  .count() \
  .orderBy("day_of_week") \
  .show()

# **Note:** Day 0 represents Monday.


# (3) Use the built-in function `dayofweek` to compute the number of rides by
# day of week.

from pyspark.sql.functions import dayofweek
rides_clean \
  .withColumn("dayofweek", dayofweek("date_time")) \
  .groupBy("dayofweek") \
  .count() \
  .orderBy("dayofweek") \
  .show()

# **Note:** Day 1 represents Sunday.


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Reading and Writing DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we introduce the DataFrameReader and DataFrameWriter classes
# and demonstrate how to read from and write to a number of data sources.


# ## Reading and Writing Data

# * Spark can read from and write to a variety of data sources.

# * The Spark SQL `DataFrameReader` and `DataFrameWriter` classes support the
# following data sources:
#   * text
#   * delimited text
#   * JSON (JavaScript Object Notation)
#   * Apache Parquet
#   * Apache ORC
#   * Apache Hive
#   * JDBC connection

# * Spark SQL also integrates with the pandas Python package.

# * Additional data sources are supported by [third-party
# packages](https://spark-packages.org/).


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# This configuration parameter 
# `spark.hadoop.fs.s3a.aws.credentials.provider`
# is required to read data from
# a public Amazon S3 bucket in the section below entitled
# **Working with object stores**.

# Create an HDFS directory for saved data:
!hdfs dfs -rm -r -skipTrash $S3_HOME/data  # Remove any existing directory
!hdfs dfs -mkdir $S3_HOME/data


# ## Working with delimited text files

# Use the
# [csv](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv)
# method of the
# [DataFrameReader](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)
# class to read a delimited text file:
riders = spark \
  .read \
  .csv(S3_ROOT + "/duocar/raw/riders/", sep=",", header=True, inferSchema=True) 

# The `csv` method is a convenience method for the following more general
# syntax:
riders = spark \
  .read \
  .format("csv") \
  .option("sep", ",") \
  .option("header", True) \
  .option("inferSchema", True) \
  .load(S3_ROOT + "/duocar/raw/riders/")

# **Note:** If you use either syntax with `header` set to `True`, then Spark
# assumes that *every* file in the directory has a header row.

# Spark does its best to infer the schema from the header row and column
# values:
riders.printSchema()

# Alternatively, you can manually specify the schema.  First, import the Spark
# SQL
# [types](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)
# module:
from pyspark.sql.types import *

# Then specify the schema as a `StructType` instance: 
schema = StructType([
    StructField("id", StringType(), True),
    StructField("birth_date", DateType(), True),
    StructField("join_date", DateType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("student", IntegerType(), True),
    StructField("home_block", StringType(), True),
    StructField("home_lat", DoubleType(), True),
    StructField("home_lon", DoubleType(), True),
    StructField("work_lat", DoubleType(), True),
    StructField("work_lon", DoubleType(), True)
])

# Finally, pass the schema to the `DataFrameReader`:
riders2 = spark \
  .read \
  .format("csv") \
  .option("sep", ",") \
  .option("header", True) \
  .schema(schema) \
  .load(S3_ROOT + "/duocar/raw/riders/")

# **Note:** We must include the header option otherwise Spark will read the
# header row as a valid record.

# Confirm the explicit schema:
riders2.printSchema()

# Use the `csv` method of the
# [DataFrameWriter](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)
# class to write the DataFrame to a tab-delimited file:
riders2.write.csv(S3_HOME + "/data/riders_tsv/", sep="\t")
!hdfs dfs -ls $S3_HOME/data/riders_tsv

# **Note:** The file has a `csv` extension even though it includes
# tab-separated values.  Never trust a file extension!

# Use the `mode` argument to overwrite existing files and the `compression`
# argument to specify a compression codec:
riders2.write.csv(S3_HOME + "/data/riders_tsv_compressed/", sep="\t", mode="overwrite", compression="bzip2")
!hdfs dfs -ls $S3_HOME/data/riders_tsv_compressed

# See the Cloudera documentation on [Data
# Compression](https://docs.cloudera.com/documentation/enterprise/latest/topics/introduction_compression.html)
# for more details.


# ## Working with text files

# Use the
# [text](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.text)
# method of the `DataFrameReader` class to read an unstructured text file:
weblogs = spark.read.text(S3_ROOT + "/duocar/earcloud/apache_logs/")
weblogs.printSchema()
weblogs.head(5)

# **Note:** The default filesystem in Hadoop (and by extension CML Native Workbench) is HDFS.
# The read statement above is a shortcut for
#```python
#weblogs = spark.read.text("hdfs:///duocar/earcloud/apache_logs/")
#```
# which in turn is a shortcut for
#```python
#weblogs = spark.read.text("hdfs:/<host:port>//duocar/earcloud/apache_logs")
#```
# where `<host:port>` is the host and port of the HDFS namenode.

# Parse the unstructured data:
from pyspark.sql.functions import regexp_extract
requests = weblogs.select(regexp_extract("value", "^.*\"(GET.*?)\".*$", 1).alias("request")) 
requests.head(5)

# Use the
# [text](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.text)
# method of the `DataFrameWriter` class to write an unstructured text file:
requests.write.text(S3_HOME + "/data/requests_txt/")
!hdfs dfs -ls $S3_HOME/data/requests_txt


# ## Working with Parquet files

# [Parquet](https://parquet.apache.org/) is a very popular columnar storage
# format for Hadoop.  Parquet is the default file format in Spark SQL.  Use
# the `parquet` method of the `DataFrameWriter` class to write to a Parquet
# file:
riders2.write.parquet(S3_HOME + "/data/riders_parquet/")
!hdfs dfs -ls $S3_HOME/data/riders_parquet

# **Note:** The SLF4J messages are a known issue with CDH.  You can safely
# ignore them.

# Use the `parquet` method of the `DataFrameReader` class to the read from a
# Parquet file:
spark.read.parquet(S3_HOME + "/data/riders_parquet/").printSchema()

# **Note:** Spark uses the schema stored with the data.


# ## Working with Hive Tables

# Use the `sql` method of the `SparkSession` class to run Hive queries:
spark.sql("SHOW DATABASES").show()
spark.sql("USE duocar")
spark.sql("SHOW TABLES").show()
spark.sql("DESCRIBE riders").show()
spark.sql("SELECT * FROM riders LIMIT 10").show()

# Use the `table` method of the `DataFrameReader` class to read a Hive table:
riders_table = spark.read.table("riders")
riders_table.printSchema()
riders_table.show(5)

# Use the `saveAsTable` method of the `DataFrameWriter` class to write a Hive
# table:
import uuid
table_name = "riders_" + str(uuid.uuid4().hex)  # Create unique table name.
riders.write.saveAsTable(table_name)


# You can now manipulate this table with Hive or Impala or via Spark SQL:
spark.sql("DESCRIBE %s" % table_name).show()


# ## Working with object stores

# Pass the appropriate prefix and path to the DataFrameReader and
# DataFrameWriter methods to read from and write to an object store.  For
# example, use the prefix `s3a` and pass the S3 bucket to read from Amazon S3:

demographics = spark.read.csv(S3_ROOT + "/duocar/raw/demographics/", sep="\t", header=True, inferSchema=True)
demographics.printSchema()
demographics.show(5)

# If we have write permissions, then we can also write files to Amazon S3 using
# the `s3a` prefix.

# **Important:** This code will fail when running Spark via YARN unless the
# worker nodes have access to the appropriate AWS credentials.  See the
# documentation for your distribution of Hadoop for more details on accessing
# cloud storage.


# ## Working with pandas DataFrames

# Import the pandas package:
import pandas as pd

# # Use the pandas `read_csv` method to read a local tab-delimited file:
# demographics_pdf = pd.read_csv(S3_ROOT + "/data/demographics.txt", sep="\t")

# # Access the pandas `dtypes` attribute to the view the data types:
# demographics_pdf.dtypes

# # Use the pandas `head` method to view the data:
# demographics_pdf.head()

# # Use the `createDataFrame` method of the `SparkSession` class to create a Spark
# # DataFrame from a pandas DataFrame:
# demographics = spark.createDataFrame(demographics_pdf)
# demographics.printSchema()
# demographics.show(5)

# Use the `toPandas` method to read a Spark DataFrame into a pandas DataFrame:
riders_pdf = riders.toPandas()
riders_pdf.dtypes
riders_pdf.head()

# **WARNING:** Use this with caution as you may use all your available memory!

# **Note:** Column types may not convert as expected when reading a Spark
# DataFrame into a pandas DataFrame and vice versa.


# ## Exercises

# (1) Use the `json` method of the `DataFrameWriter` class to write the
# `riders` DataFrame to the `data/riders_json/` (HDFS) directory.

# (2) Use the `hdfs dfs -ls` command to list the contents of the
# `data/riders_json/` directory.

# (3) Use the `hdfs dfs -cat` and `head` commands to display a JSON file in
# the `data/riders_json` directory.

# (4) Use Hue to browse the `data/riders_json/` directory.

# (5) Use the `json` method of the `DataFrameReader` class to read the JSON
# file into a DataFrame.

# (6) Examine the schema of the DataFrame.  Do you notice anything different?


# ## References

# [Spark Python API - pyspark.sql.DataFrameReader
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)

# [Spark Python API - pyspark.sql.DataFrameWriter
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)


# ## Cleanup

# Drop the Hive table:
spark.sql("DROP TABLE IF EXISTS %s" % table_name)

# Stop the SparkSession:
spark.stop()
# # Reading and Writing DataFrames - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Set environment variables
from env import S3_ROOT, S3_HOME, CONNECTION_NAME
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw rider data from HDFS:
riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)


# ## Exercises

# (1) Use the `json` method of the `DataFrameWriter` class to write the
# `riders` DataFrame to the `data/riders_json/` (HDFS) directory.

riders.write.json(S3_HOME + "/data/riders_json/", mode="overwrite")

# (2) Use the `hdfs dfs -ls` command to list the contents of the
# `data/riders_json/` directory.

!hdfs dfs -ls $S3_HOME/data/riders_json

# (3) Use the `hdfs dfs -cat` and `head` commands to display a JSON file in
# the `data/riders_json` directory.

!hdfs dfs -cat $S3_HOME/data/riders_json/part* | head -n 5

# (4) Use Hue to browse the `data/riders_json/` directory.

# Note that the JSON file is noticeably larger than the original CSV file.

# (5) Use the `json` method of the `DataFrameReader` class to read the JSON
# file into a DataFrame.

riders_json = spark.read.json(S3_HOME + "/data/riders_json/")

# (6) Examine the schema of the DataFrame.  Do you notice anything different?

riders_json.printSchema()

# Note that the columns are in alphabetical order.


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Combining and Splitting DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to combine and split DataFrames.


# ## Combining and Splitting DataFrames

# * Spark SQL supports the usual database-style joins:
#   * Cross join
#   * Inner join
#   * Left semi join
#   * Left anti join
#   * Left outer join
#   * Right outer join
#   * Full outer join

# * Joins are expensive in the big-data world
#   * Perform joins early in the process
#   * Amortize the cost over many use cases

# * Spark SQL supports the following set operations:
#   * Union
#   * Intersection
#   * Subtraction

# * Spark SQL provides a method to split a DataFrame into random subsets


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# ## Joining DataFrames

# We will use the following DataFrames to demonstrate joins:

scientists = spark.read.csv(S3_ROOT + "/duocar/raw/data_scientists/", header=True, inferSchema=True)
scientists.show()

offices = spark.read.csv(S3_ROOT + "/duocar/raw/offices/", header=True, inferSchema=True)
offices.show()

# ### Cross join

# Use the `crossJoin` DataFrame method to join every row in the left
# (`scientists`) DataFrame with every row in the right (`offices`) DataFrame:
scientists.crossJoin(offices).show()

# **Warning:** This can result in very big DataFrames!

# **Note:** Columns with the same name are not renamed.

# **Note:** This is called the *Cartesian product* of the two DataFrames.

# ### Inner join

# Use the `join` DataFrame method with different values of the `how` argument
# to perform other types of joins.

# Use a join expression and the value `inner` to return only those rows for
# which the join expression is true:
scientists.join(offices, scientists.office_id == offices.office_id, "inner").show()

# This gives us a list of data scientists associated with an office and the
# corresponding office information.

# Since the join key has the same name on both DataFrames, we can simplify the
# join as follows:
scientists.join(offices, "office_id", "inner").show()

# Since an inner join is the default, we can further simplify the join as
# follows:
scientists.join(offices, "office_id").show()

# ### Left semi join

# Use the value `left_semi` to return the rows in the left DataFrame that match
# rows in the right DataFrame:
scientists \
  .join(offices, scientists.office_id == offices.office_id, "left_semi") \
  .show()

# This gives us a list of data scientists associated with an office.

# ### Left anti join

# Use the value `left_anti` to return the rows in the left DataFrame that do
# not match rows in the right DataFrame:
scientists \
  .join(offices, scientists.office_id == offices.office_id, "left_anti") \
  .show()

# This gives us a list of data scientists not associated with an office.

# **Note:** You can think of the left semi and left anti joins as special types
# of filters.

# ### Left outer join

# Use the value `left` or `left_outer` to return every row in the left
# DataFrame with or without matching rows in the right DataFrame:
scientists \
  .join(offices, scientists.office_id == offices.office_id, "left_outer") \
  .show()

# This gives us a list of data scientists with or without an office.

# ### Right outer join

# Use the value `right` or `right_outer` to return every row in the right
# DataFrame with or without matching rows in the left DataFrame:
scientists \
  .join(offices, scientists.office_id == offices.office_id, "right_outer") \
  .show()

# This gives us a list of offices with or without a data scientist.

# **Note:** The Paris office has two data scientists.

# ### Full outer join

# Use the value `full`, `outer`, or `full_outer` to return the union of the
# left outer and right outer joins (with duplicates removed):
scientists \
  .join(offices, scientists.office_id == offices.office_id, "full_outer") \
  .show()

# This gives us a list of all data scientists whether or not they have an
# office and all offices whether or not they have any data scientists.

# ### Example: Joining the DuoCar data

# Let us join the driver, rider, and review data with the ride data.

# Read the clean data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
drivers = spark.read.parquet(S3_ROOT + "/duocar/clean/drivers/")
riders = spark.read.parquet(S3_ROOT + "/duocar/clean/riders/")
reviews = spark.read.parquet(S3_ROOT + "/duocar/clean/ride_reviews/")

# Since we want all the ride data, we will use a sequence of left outer joins:
joined = rides \
  .join(drivers, rides.driver_id == drivers.id, "left_outer") \
  .join(riders, rides.rider_id == riders.id, "left_outer") \
  .join(reviews, rides.id == reviews.ride_id, "left_outer")
joined.printSchema()

# **Note:** We probably want to rename some columns before joining the data and
# remove the duplicate ID columns after joining the data to make this DataFrame
# more usable.  For example, see the `joined` data in the DuoCar data
# repository:
spark.read.parquet(S3_ROOT + "/duocar/joined/").printSchema()


# ## Applying set operations to DataFrames

# Spark SQL provides the following DataFrame methods that implement set
# operations:
# * `union`
# * `intersect`
# * `subtract`

# Use the `union` method to get the union of rows in two DataFrames with
# similar schema:
driver_names = drivers.select("first_name")
driver_names.count()

rider_names = riders.select("first_name")
rider_names.count()

names_union = driver_names.union(rider_names).orderBy("first_name")
names_union.count()
names_union.show()

# Note that `union` does not remove duplicates.  Use the `distinct` method to
# remove duplicates:
names_distinct = names_union.distinct()
names_distinct.count()
names_distinct.show()

# Use the `intersect` method to return rows that exist in both DataFrames:
name_intersect = driver_names.intersect(rider_names).orderBy("first_name")
name_intersect.count()
name_intersect.show()

# Use the `subtract` method to return rows in the left DataFrame that do not
# exist in the right DataFrame:
names_subtract = driver_names.subtract(rider_names).orderBy("first_name")
names_subtract.count()
names_subtract.show()


# ## Splitting a DataFrame

# Use the `randomSplit` DataFrame method to split a DataFrame into random
# subsets:
riders.count()
(train, validate, test) = riders.randomSplit(weights=[0.6, 0.2, 0.2])
(train.count(), validate.count(), test.count())

# Use the `seed` argument to ensure replicability:
(train, validate, test) = riders.randomSplit([0.6, 0.2, 0.2], seed=12345)
(train.count(), validate.count(), test.count())

# If the proportions do not add up to one, then Spark will normalize the values:
(train, validate, test) = riders.randomSplit([60.0, 20.0, 20.0], seed=12345)
(train.count(), validate.count(), test.count())

# **Note:** The weights must be doubles.

# **Note:** The same seed will result in the same random split.


# ## Exercises

# (1) Join the `rides` DataFrame with the `reviews` DataFrame.  Keep only those
# rides that have a review.

# (2) How many drivers have not provided a ride?


# ## References

# [Spark Python API - crossJoin DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.crossJoin)

# [Spark Python API - join DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.join)

# [Spark Python API - union DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.union)

# [Spark Python API - intersect DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.intersect)

# [Spark Python API - subtract DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.subtract)

# [Spark Python API - randomSplit DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.randomSplit)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Combining and Splitting DataFrames - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the clean data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
drivers = spark.read.parquet(S3_ROOT + "/duocar/clean/drivers/")
reviews = spark.read.parquet(S3_ROOT + "/duocar/clean/ride_reviews/")


# ## Exercises

# (1) Join the `rides` DataFrame with the `reviews` DataFrame.  Keep only those
# rides that have a review.

# Count the number of reviews before the join:
reviews.count()

# Perform a right outer join:
rides_with_reviews = rides.join(reviews, rides.id == reviews.ride_id, "right_outer")

# Count the number of reviews after the join:
rides_with_reviews.count()

# Print the schema:
rides_with_reviews.printSchema()

# (2) How many drivers have not provided a ride?

# Get the driver IDs from `drivers` DataFrame:
id_from_drivers = drivers.select("id")

# Get the driver IDs from `rides` DataFrame:
id_from_rides = rides.select("driver_id").withColumnRenamed("driver_id", "id")

# Find lazy drivers using a left anti join:
lazy_drivers1 = id_from_drivers.join(id_from_rides, "id", "left_anti")
lazy_drivers1.count()
lazy_drivers1.orderBy("id").show(5)

# Find lazy drivers using a subtraction:
lazy_drivers2 = id_from_drivers.subtract(id_from_rides)
lazy_drivers2.count()
lazy_drivers2.orderBy("id").show(5)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Summarizing and Grouping DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to summarize, group, and pivot data in a DataFrame.

# * Summarizing data with aggregate functions
# * Grouping data
# * Pivoting data


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the enhanced (joined) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")

# Since we will be querying the `rides` DataFrame many times, let us persist
# it in memory to improve performance:
rides.persist()


# ## Summarizing data with aggregate functions

# Spark provides a number of summarization (aggregate) functions.  For example,
# the `describe` method provides basic summary statistics:
rides.describe("distance").show()

# Use the `count`, `countDistinct`, and `approx_count_distinct` functions to
# compute various column counts:
from pyspark.sql.functions import count, countDistinct, approx_count_distinct
rides.select(count("*"), count("distance"), countDistinct("distance"), approx_count_distinct("distance")).show()

# **Note:** The `count` function returns the number of rows with non-null values.

# **Note:** Use `count(lit(1))` rather than `count(1)` as an alternative to `count("*")`.

# The `agg` method returns the same results and can be applied to grouped data:
rides.agg(count("*"), count("distance"), countDistinct("distance"), approx_count_distinct("distance")).show()

# Use the `sum` and `sumDistinct` functions to compute various column sums:
from pyspark.sql.functions import sum, sumDistinct
rides.agg(sum("distance"), sumDistinct("distance")).show()

# **Question:** When would one use the `sumDistinct` function?

# Spark SQL provides a number of summary statistics:
from pyspark.sql.functions import mean, stddev, variance, skewness, kurtosis
rides.agg(mean("distance"), stddev("distance"), variance("distance"), skewness("distance"), kurtosis("distance")).show()

# **Note:** `mean` is an alias for `avg`, `stddev` is an alias for the sample
# standard deviation `stddev_samp`, and `variance` is an alias for the sample
# variance `var_samp`.  The population standard deviation and population
# variance are available via `stddev_pop` and `var_pop`, respectively.

# Use the `min` and `max` functions to compute the minimum and maximum, respectively:
from pyspark.sql.functions import min, max
rides.agg(min("distance"), max("distance")).show()

# Use the `first` and `last` functions to compute the first and last values, respectively:
from pyspark.sql.functions import first, last
rides \
  .orderBy("distance") \
  .agg(first("distance", ignorenulls=False), last("distance", ignorenulls=False)) \
  .show()

# **Note:** Null values sort before valid numerical values.

# Use the `corr`, `covar_samp`, or `covar_pop` functions to measure the linear
# association between two columns:
from pyspark.sql.functions import corr, covar_samp, covar_pop
rides \
  .agg(corr("distance", "duration"), covar_samp("distance", "duration"), covar_pop("distance", "duration")) \
  .show()

# The `collect_list` and `collect_set` functions return a column of array type:
from pyspark.sql.functions import collect_list, collect_set
rides.agg(collect_set("service")).show(truncate=False)

# **Note:** `collect_list` does not remove duplicates and will return a very
# long array in this case.


# ## Grouping data

# Use the `agg` method with the `groupBy` (or `groupby`) method to refine your
# analysis:
rides \
  .groupBy("rider_student") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .show()

# You can use more than one column in the `groupBy` method:
rides \
  .groupBy("rider_student", "service") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()

# Use the `rollup` method to get some subtotals:
rides \
  .rollup("rider_student", "service") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()

# Use the `grouping` function to identify grouped rows:
from pyspark.sql.functions import grouping
rides \
  .rollup("rider_student", "service") \
  .agg(grouping("rider_student"), grouping("service"), count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()

# Use the `cube` method to get all subtotals:
rides \
  .cube("rider_student", "service") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()

# Use the `grouping_id` function to identify grouped rows:
from pyspark.sql.functions import grouping_id
rides \
  .cube("rider_student", "service") \
  .agg(grouping_id("rider_student", "service"), count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()


# ## Pivoting data

# The following use case is common:
rides.groupBy("rider_student", "service").count().orderBy("rider_student", "service").show()

# The `crosstab` method can be used to present this result in a pivot table:
rides.crosstab("rider_student", "service").show()

# We can also use the `pivot` method to produce a cross-tabulation:
rides.groupBy("rider_student").pivot("service").count().show()

# We can also perform other aggregations:
rides.groupBy("rider_student").pivot("service").mean("distance").show()

rides.groupBy("rider_student").pivot("service").agg(mean("distance")).show()

# You can explicitly choose the values that are pivoted to columns:
rides.groupBy("rider_student").pivot("service", ["Car", "Grand"]).agg(mean("distance")).show()

# Additional aggregation functions produce additional columns:
rides.groupBy("rider_student").pivot("service", ["Car"]).agg(count("distance"), mean("distance")).show()


# ## Exercises

# (1) Who are DuoCar's top 10 riders in terms of number of rides taken?

# (2) Who are DuoCar's top 10 drivers in terms of total distance driven?

# (3) Compute the distribution of cancelled rides.

# (4) Compute the distribution of ride star rating.
# When is the ride star rating missing?

# (5) Compute the average star rating for each level of car service.
# Is the star rating correlated with the level of car service?


# ## References

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)

# [Spark Python API - pyspark.sql.GroupedData
# class](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData)


# ## Cleanup

# Unpersist the DataFrame:
rides.unpersist()

# Stop the SparkSession:
spark.stop()
# # Summarizing and Grouping DataFrames - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the enhanced (joined) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")

# Since we will be querying the `rides` DataFrame many times, let us persist
# it in memory to improve performance:
rides.persist()


# ## Exercises

# (1) Who are DuoCar's top 10 riders in terms of number of rides taken?

from pyspark.sql.functions import col
rides \
  .filter(col("cancelled") == False) \
  .groupBy("rider_id") \
  .count() \
  .orderBy(col("count").desc()) \
  .show(10)

# (2) Who are DuoCar's top 10 drivers in terms of total distance driven?

from pyspark.sql.functions import sum
rides \
  .groupBy("driver_id") \
  .agg(sum("distance").alias("sum_distance")) \
  .orderBy(col("sum_distance").desc()) \
  .show(10)

# (3) Compute the distribution of cancelled rides.

rides.groupBy("cancelled").count().show()

# (4) Compute the distribution of ride star rating.
# When is the ride star rating missing?

rides.groupBy("star_rating").count().orderBy("star_rating").show()

# The star rating is missing when a ride is cancelled:
rides.crosstab("star_rating", "cancelled").orderBy("star_rating_cancelled").show()

# (5) Compute the average star rating for each level of car service.
# Is the star rating correlated with the level of car service?

rides.groupBy("service").mean("star_rating").show()


# ## Cleanup

# Unpersist the DataFrame:
rides.unpersist()

# Stop the SparkSession:
spark.stop()
# # Window Functions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to create and apply window functions.


# ## Window Functions

# * Spark SQL supports the following window functions:
#   * `cume_dist`
#   * `dense_rank`
#   * `lag`
#   * `lead`
#   * `ntile`
#   * `percent_rank`
#   * `rank`
#   * `row_number`

# * Aggregate and window functions are applied `over` a window specification

# * A window specification consists of at least one of the following:
#   * Partitioning column
#   * Ordering column
#   * Row specification


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Example: Cumulative Count and Sum

# Create a simple DataFrame:
df = spark.range(10)
df.show()

# Create a simple window specification:
from pyspark.sql.window import Window
ws = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
type(ws)

# Use the window specification to compute cumulative count and sum:
from pyspark.sql.functions import count, sum
df.select("id", count("id").over(ws).alias("cum_cnt"), sum("id").over(ws).alias("cum_sum")).show()

# **Tip:** Examine the default column name to gain additional insight (if you
# are SQL literate):
df.select("id", count("id").over(ws), sum("id").over(ws)).printSchema()


# ## Example: Compute average days between rides for each rider

# Create window specification:
ws = Window.partitionBy("rider_id").orderBy("date_time")

# Use the `lag` function to extract the date and time of the previous ride:
from pyspark.sql.functions import lag
rides2 = rides.withColumn("date_time_previous", lag("date_time").over(ws))
rides2.select("rider_id", "date_time", "date_time_previous").show(truncate=False)

# **Note:** A rider's first ride does not have a previous ride; therefore, the
# value is set to null.

# Compute the number of days between consecutive rides:
from pyspark.sql.functions import datediff
rides3 = rides2.withColumn("days_between_rides", datediff("date_time", "date_time_previous"))
rides3.select("rider_id", "date_time", "date_time_previous", "days_between_rides").show(truncate=False)

# Compute the average days between rides for each rider:
from pyspark.sql.functions import count, mean
rides4 = rides3 \
  .groupBy("rider_id") \
  .agg(count("*").alias("num_rides"), mean("days_between_rides").alias("mean_days_between_rides"))

# Compute top and bottom 10 riders:
rides4 \
  .where(rides4.mean_days_between_rides.isNotNull()) \
  .orderBy("mean_days_between_rides") \
  .show(10)

rides4 \
  .orderBy("mean_days_between_rides", ascending=False) \
  .show(10)

# **Question:** How can we make this analysis better?


# ## Exercises

# (1) What is the average time between rides for each driver?


# ## References

# [Spark Python API - pyspark.sql.Window
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.Window)

# [Spark Python API - pyspark.sql.WindowSpec
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.WindowSpec)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Window Functions - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Exercises

# (1) What is the average time between rides for each driver?

# Define the window specification:
from pyspark.sql.window import Window
driver_ws = Window.partitionBy("driver_id").orderBy("date_time")

# Generate date and time of previous ride, number of days between current ride
# and previous ride, and mean days between rides for each driver:
from pyspark.sql.functions import count, datediff, lag, mean
driver_ride_days = rides \
  .withColumn("date_time_previous", lag("date_time").over(driver_ws)) \
  .withColumn("days_between_rides", datediff("date_time", "date_time_previous")) \
  .groupBy("driver_id") \
  .agg(count("*").alias("num_rides"), mean("days_between_rides").alias("mean_days_between_rides"))

# List busy and not-so-busy drivers:
driver_ride_days.orderBy("mean_days_between_rides").show(10)
driver_ride_days.orderBy("mean_days_between_rides", ascending=False).show(10)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Exploring and Visualizing DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# Now that we have enhanced our ride data, we can begin a more systematic
# exploration of the relationships among the variables.  The insight gathered
# during this analysis may be used to improve DuoCar's day-to-day business
# operations or it may serve as preparation for more sophisticated analysis and
# modeling using machine learning algorithms.  In this module we use Spark in
# conjunction with some popular Python libraries to explore the DuoCar data.


# ## Possible work flows for big data

# * Work with all of the data on the cluster
#   * Produces accurate reports
#   * Limits analysis to tabular reports
#   * Requires more computation
# * Work with a sample of the data in local memory
#   * Opens up a wide range of tools
#   * Enables more rapid iterations
#   * Produces sampled results
# * Summarize on the cluster and visualize summarized data in local memory
#   * Produces accurate counts
#   * Allows for wide range of analysis and visualization tools


# ## Setup

# Import useful packages, modules, classes, and functions:
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Create a SparkSession:
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Load the enhanced ride data from HDFS:
rides_sdf = spark.read.parquet(S3_ROOT + "/duocar/joined_all")

# Create a random sample and load it into a pandas DataFrame:
rides_pdf = rides_sdf.sample(withReplacement=False, fraction=0.01, seed=12345).toPandas()

# **Note:** In this module we will use `sdf` to denote Spark DataFrames and
# `pdf` to denote pandas DataFrames.


# ## Exploring a single variable

# In this section we use Spark and Spark in conjunction with pandas,
# matplotlib, and seaborn to explore a single variable (i.e., column).  Many of
# the techniques presented here can be useful when inspecting variables too.

# ### Exploring a categorical variable

# Let us explore type of car service, which is an example of a categorical
# variable.

# We can use the `groupBy` method in Spark to create a one-way frequency table:
summarized_sdf = rides_sdf.groupBy("service").count().orderBy("service")
summarized_sdf.show()

# We can convert the grouped Spark DataFrame to a pandas DataFrame:
summarized_pdf = summarized_sdf.toPandas()
summarized_pdf

# **Note**: Remember that we are loading data into local memory.  In this case
# we are safe since the summarized DataFrame is small.

# Specify the desired order of the categories:
order = ["Car", "Noir", "Grand", "Elite"]

# Use the seaborn package to plot the *summarized* data:
sns.barplot(x="service", y="count", data=summarized_pdf, order=order)

# Use the seaborn package to plot the *sampled* data:
sns.countplot(x="service", data=rides_pdf, order=order)

# **Note:** The plots present the same qualitative information.


# ### Exploring a continuous variable

# We can use the `describe` method to compute basic summary statistics:
rides_sdf.describe("distance").show()

# and aggregate functions to get additional summary statistics:
rides_sdf.agg(skewness("distance"), kurtosis("distance")).show()

# We can use the `approxQuantile` method to compute approximate quantiles:
rides_sdf.approxQuantile("distance", probabilities=[0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0], relativeError=0.1)

# **Note:** Set `relativeError = 0.0` for exact (and possibly expensive)
# quantiles.

# However, a histogram is generally more informative than summary statistics.
# Create an approximate 1% random sample:
sampled_pdf = rides_sdf \
  .select("distance") \
  .dropna() \
  .sample(withReplacement=False, fraction=0.01, seed=23456) \
  .toPandas()

# Use seaborn to create a histogram on the *sampled* data:
sns.distplot(sampled_pdf["distance"], kde=False)

# Use seaborn to create a *normalized* histogram with rug plot and kernel
# density estimate:
sns.distplot(sampled_pdf["distance"], kde=True, rug=True)

# Use seaborn to create a boxplot, which displays much of the information
# computed via the `approxQuantile` method:
sns.boxplot(x="distance", data=sampled_pdf)


# ## Exploring a pair of variables

# ### Categorical-Categorical

# Let us explore the distribution of a rider's gender by student status.

# Create a two-way frequency table:
summarized_sdf = rides_sdf.groupBy("rider_student", "rider_gender").count().orderBy("rider_student", "rider_gender")
summarized_sdf.show()

# Convert the *summarized* Spark DataFrame to a pandas DataFrame:
summarized_pdf = summarized_sdf.toPandas()
summarized_pdf

# Produce a bar chart using seaborn:
hue_order = ["female", "male"]
sns.barplot(x="rider_student", y="count", hue="rider_gender", hue_order=hue_order, data=summarized_pdf)

# Replace missing values:
summarized_pdf = summarized_sdf.fillna("missing").toPandas()
hue_order = ["female", "male", "missing"]
sns.barplot(x="rider_student", y="count", hue="rider_gender", hue_order=hue_order, data=summarized_pdf)

# ### Categorical-Continuous

# Let us explore the distribution of ride distance by rider student status.

# We can produce tabular reports in Spark:
rides_sdf \
  .groupBy("rider_student") \
  .agg(count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student") \
  .show()

# Alternatively, we can produce visualizations on a sample:
sampled_pdf = rides_sdf \
  .select("rider_student", "distance") \
  .sample(withReplacement=False, fraction=0.01, seed=34567) \
  .toPandas()

# Use seaborn to produce a strip plot on the sampled data:
sns.stripplot(x="rider_student", y="distance", data=sampled_pdf, jitter=True)

# **Note:** Non-students are taking the long rides.

# See the supplement for other ways to visualize this data.


# ### Continuous-Continuous

# Use the `corr`, `covar_samp`, and `covar_pop` aggregate functions to measure
# the linear relationship between two variables:
rides_sdf.agg(corr("distance", "duration"),
              covar_samp("distance", "duration"),
              covar_pop("distance", "duration")).show()

# Use the `jointplot` function to produce an enhanced scatter plot on the
# *sampled* data:
sns.jointplot(x="distance", y="duration", data=rides_pdf)

# Overlay a linear regression model:
sns.jointplot(x="distance", y="duration", data=rides_pdf, kind="reg")

# Overlay a quadratic regression model (order = 2):
sns.jointplot(x="distance", y="duration", data=rides_pdf, kind="reg", order=2)

# Use the `pairplot` function to examine several pairs at once:
sampled_pdf = rides_sdf \
  .select("distance", "duration", hour("date_time")) \
  .dropna() \
  .sample(withReplacement=False, fraction=0.01, seed=45678) \
  .toPandas()
sns.pairplot(sampled_pdf)


# ## Exercises

# (1) Look for variables that might help us predict ride duration.

# (2) Look for variables that might help us predict ride rating.


# ## References

# [The SciPy Stack](https://scipy.org/)

# [pandas](http://pandas.pydata.org/)

# [matplotlib](https://matplotlib.org/index.html)

# [seaborn](https://seaborn.pydata.org/index.html)

# [Bokeh](http://bokeh.pydata.org/en/latest/)

# [Plotly](https://plot.ly/)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Exploring and Visualizing DataFrames - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Import useful packages, modules, classes, and functions:
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Load the enhanced ride data from HDFS:
rides_sdf = spark.read.parquet(S3_ROOT + "/duocar/joined_all")

# Create a random sample and load it into a pandas DataFrame:
rides_pdf = rides_sdf.sample(withReplacement=False, fraction=0.01, seed=12345).toPandas()


# ## Exercises

# (1) Look for variables that might help us predict ride duration.

# Does the ride duration depend on the day of the week?

# Produce a summary table:
result1 = rides_sdf \
  .groupBy(F.dayofweek("date_time").alias("day_of_week")) \
  .agg(F.count("duration"), F.mean("duration")) \
  .orderBy("day_of_week")
result1.show()

# Plot the summarized data:
result1_pdf = result1.toPandas()
sns.barplot(x="day_of_week", y="avg(duration)", data=result1_pdf)

# (2) Look for variables that might help us predict ride rating.

# Do elite vehicles get higher ratings?

# Produce a summary table:
result2 = rides_sdf \
  .groupBy("vehicle_elite") \
  .agg(F.count("star_rating"), F.mean("star_rating")) \
  .orderBy("vehicle_elite")
result2.show()

# Plot the summarized data:
result2_pdf = result2.toPandas()
sns.barplot(x="vehicle_elite", y="avg(star_rating)", data=result2_pdf)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Monitoring, Tuning, and Configuring Spark Applications

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Monitoring Spark Applications

# We monitor a *Spark application* via the *Spark UI*.  The Spark UI is not
# available until we start a Spark application.  We start a Spark application
# by creating a `SparkSession` instance:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# A link to the Spark UI is available at the top of the CML Native Workbench console pane.

# **Important:** If the Spark UI link brings up a blank page, then you can
# access the Spark UI via the Spark History Server (SHS) or directly at
#```
#http://spark-<session>.cdsw-gateway.<cluster>.duocar.us/
#```
# where `session` and `cluster` are listed in the session URL
#```
#http://cdsw-gateway.<cluster>.duocar.us/<user>/<project>/engines/<session>
#```


# ### Example 1: Partitioning DataFrames

# Read the ride data as a text file:
rides = spark.read.text(S3_ROOT + "/duocar/raw/rides/")

# View the Spark UI and note that this operation does not generate a *Spark
# job*.

# Get the number of partitions:
rides.rdd.getNumPartitions()

# Note that we are accessing the [Resilient Distributed Dataset
# (RDD)](http://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds)
# underlying the DataFrame.

# Print the schema:
rides.printSchema()

# Note that this operation also does not generate a job.

# Print a few rows:
rides.show(5)

# Note that `show` is an *action* and generates a job with one *stage* and one
# *task*.  Show is actually a *partial action* since Spark does not have to
# read all the data to print out a few rows.

# `count` is also an action:
rides.count()

# Note that `count` generates a job with two stages and three tasks.  The first
# stage consists of two parallel tasks that count the number of rows in each
# partition.  The second stage consists of one task that adds these partial sum
# to compute the final count.

# Save the DataFrame to HDFS:
rides.write.mode("overwrite").text(S3_HOME + "/data/monitor/")

# Note that each partition is written to a separate file.

# Let us repartition the DataFrame into six partitions:
rides6 = rides.repartition(6)

# Count the number of rows:
rides6.count()

# Note that repartitioning the DataFrame requires shuffling data and therefore
# generates an additional stage.

# The `coalesce` method is a more efficient way to reduce the number of
# partitions:
rides.coalesce(1).write.mode("overwrite").text(S3_HOME + "/data/monitor/")

# Here we have used the `coalesce` method to reduce the number of partitions
# before writing the DataFrame to HDFS.

# Remove the temporary file:
!hdfs dfs -rm -r $S3_HOME/data/monitor/


# ### Example 2: Persisting DataFrames

# Read the ride data as a (comma) delimited text file:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides", header=True, inferSchema=True)

# Note that Spark ran two exploratory jobs to read the header and infer the
# schema.

# Duplicate the ride data to make it bigger:
big_rides = spark.range(100).crossJoin(rides)

# Print the number of partitions:
big_rides.rdd.getNumPartitions()

# Chain together a more elaborate set of transformations:
from pyspark.sql.functions import count, mean, stddev
result = big_rides \
  .groupby("rider_id") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("count(distance)", ascending=False)

# Spark determines the appropriate number of partitions:
result.rdd.getNumPartitions()

# Persist the DataFrame in memory:
result.persist()

# Review the **Storage** tab in the Spark UI.  Spark does not persist the DataFrame
# until it is actually computed.

# Run an action to compute the DataFrame:
%time result.count()

# Note that the DataFrame is now listed under the **Storage** tab in the Spark UI.

# Run the action again:
%time result.count()

# Note that it runs noticeably faster since the result is already in memory:

# Free up memory:
result.unpersist()

# Stop the SparkSession:
spark.stop()

# This also stops the Spark Application and disables the Spark UI.


# ## Configuring the Spark Environment

# We have been creating a SparkSession using the following syntax:
#```python
#spark = SparkSession.builder \
#  .master("local") \
#  .appName("config") \
#  .getOrCreate()
#```

# This is actually a special case of the following more general syntax:
#```python
#spark = SparkSession.builder \
#  .config("spark.master", "local") \
#  .config("spark.app.name", "config") \
#  .getOrCreate()
#```

# We can configure additional environment settings:
spark = SparkSession.builder \
  .config("spark.master", "local") \
  .config("spark.app.name", "config") \
  .config("spark.driver.memory", "2g") \
  .getOrCreate()

# We can query a configuration property using the following syntax:
spark.conf.get("spark.driver.memory")

# We can view other settings under the **Environment** tab of the Spark UI.

# You can set configuration properties in the `spark-defaults.conf` file:
!cat spark-defaults.conf

# Stop the SparkSession (and the Spark application):
spark.stop()


## References

# [Monitoring Spark
# Applications](https://docs.cloudera.com/documentation/enterprise/latest/topics/operation_spark_applications.html#spark_monitoring)

# [Tuning Spark
# Applications](https://docs.cloudera.com/documentation/enterprise/latest/topics/admin_spark_tuning1.html)

# [Configuring the Cloudera Distribution of Apache Spark
# 2](https://docs.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_spark_configuration.html)
# # Fitting and Evaluating Regression Models

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * A regression algorithm is a supervised learning algorithm.
#   * The inputs are called *features*
#   * The output is called the *label*

# * A regression model provides a prediction of a continuous numerical label.

# * Spark MLlib provides several regression algorithms:
#   * Linear Regression (with Elastic Net, Lasso, and Ridge Regression)
#   * Isotonic Regression
#   * Decision Tree
#   * Random Forest
#   * Gradient-Boosted Trees

# * Spark MLlib also provides regression algorithms for special types of
# continuous numerical labels:
#   * Generalized Regression
#   * Survival Regression

# * Spark MLlib requires the features to be assembled into a vector of doubles column.


# ## Scenario

# We will build a regression model to predict the duration of a ride from
# the distance of the ride.  We can then use this regression model in our
# mobile application to provide a real-time estimate of arrival time.
# In the demonstration, we will use linear regression.  In the exercise, we will
# use isotonic regression.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ## Start a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the (clean) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
rides.printSchema()


# ## Prepare the regression data

# Select the feature and the label and filter out the cancelled rides:
regression_data = rides.select("distance", "duration").filter("cancelled = 0")


# ## Plot the data

# Plot the ride duration versus the ride distance on a random sample of rides
# using pandas:
regression_data \
  .sample(withReplacement=False, fraction=0.1, seed=12345) \
  .toPandas() \
  .plot.scatter(x="distance", y="duration")


# ## Assemble the feature vector

# Import the `VectorAssembler` class from the `pyspark.ml.feature` module:
from pyspark.ml.feature import VectorAssembler

# Create an instance of the `VectorAssembler` class:
assembler = VectorAssembler(inputCols=["distance"], outputCol="features")

# Call the `transform` method to assemble the feature vector:
assembled = assembler.transform(regression_data)

# Examine the transformed DataFrame:
assembled.printSchema()
assembled.show(5)

# **Note:** The `VectorAssembler` instance is an example of a Spark MLlib
# `Transformer`.  It takes a DataFrame as input and returns a DataFrame as
# output via the `transform` method.


# ## Create a train and test set

# Use the `randomSplit` method to create random partitions of the data:
(train, test) = assembled.randomSplit(weights=[0.7, 0.3], seed=23451)

# We will fit the regression model on the `train` DataFrame
# and evaluate it on the `test` DataFrame.


# ## Specify a linear regression model

# Import the `LinearRegression` class from the `pyspark.ml.regression` module:
from pyspark.ml.regression import LinearRegression

# Create an instance of the `LinearRegression` class:
lr = LinearRegression(featuresCol="features", labelCol="duration")

# Examine additional hyperparameters;
print(lr.explainParams())


# ## Fit the linear regression model

# Call the `fit` method to fit the linear regression model on the `train` data:
lr_model = lr.fit(train)

# The `fit` method returns an instance of the `LinearRegressionModel` class:
type(lr_model)

# **Note:** The `LinearRegression` instance is an example of a Spark MLlib
# `Estimator`.  It takes a DataFrame as input and returns a `Transformer` as
# output via the `fit` method.


# ## Examine the model parameters

# Access the `intercept` and `coefficients` attributes to get the intercept and
# slope (for each feature) of the linear regression model:
lr_model.intercept
lr_model.coefficients

# The slope is stored as a `DenseVector`.  Call the `toArray` method to convert
# the DenseVector to a NumPy array:
lr_model.coefficients.toArray()


# ## Examine various model performance measures

# The `summary` attribute is an instance of the `LinearRegressionTrainingSummary` class:
type(lr_model.summary)

# It contains a number of model performance measures:
lr_model.summary.r2
lr_model.summary.rootMeanSquaredError


# ## Examine various model diagnostics

# The `summary` attribute also contains various model diagnostics:
lr_model.summary.coefficientStandardErrors
lr_model.summary.tValues
lr_model.summary.pValues

# **Important:** The first element of each list corresponds to the slope and
# the last element corresponds to the intercept.

# **Note:** The summary attribute contains additional useful information.
  

# ## Apply the linear regression model to the test data

# Use the `transform` method to apply the linear regression model to the `test`
# DataFrame:
predictions = lr_model.transform(test)

# The `transform` method adds a column to the DataFrame with the predicted
# label:
predictions.printSchema()
predictions.show(5)


# ## Evaluate the linear regression model on the test data

# Import the `RegressionEvaluator` class from the `pyspark.ml.evaluation` module:
from pyspark.ml.evaluation import RegressionEvaluator

# Create an instance of the `RegressionEvaluator` class:
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="duration", metricName="r2")

# Call the `explainParams` method to see other metrics:
print(evaluator.explainParams())

# Use the `evaluate` method to compute the metric on the `predictions` DataFrame:
evaluator.evaluate(predictions)

# Use the `setMetricName` method to change the metric:
evaluator.setMetricName("rmse").evaluate(predictions)

# **Note:** You can also use the `evaluate` method of the `LinearRegressionModel` class.


# ## Plot the linear regression model

def plot_lr_model():
  pdf = predictions.sample(withReplacement=False, fraction= 0.1, seed=34512).toPandas()
  plt.scatter("distance", "duration", data=pdf)
  plt.plot("distance", "prediction", color="black", data=pdf)
  plt.xlabel("Distance (m)")
  plt.ylabel("Duration (s)")
  plt.title("Linear Regression Model")
plot_lr_model()


# ## Exercises

# In the following exercises we use *isotonic regression* to fit a monotonic
# function to the data.

# (1)  Import the `IsotonicRegression` class from the regression module.

# (2)  Create an instance of the `IsotonicRegression` class.  Use the same
# features and label that we used for our linear regression model.

# (3)  Fit the isotonic regression model on the train data.  Note that this
# will produce an instance of the `IsotonicRegressionModel` class.

# (4)  The model parameters are available in the `boundaries` and `predictions`
# attributes of the isotonic regression model.  Print these attributes.

# (5) Apply the isotonic regression model to the train data using the
# `transform` method.

# (6) Use the `RegressionEvaluator` to compute the RMSE on the train data.

# (7) Repeat (5) and (6) on the test data.  Compare the RMSE for the isotonic
# regression model to the RMSE for the linear regression model.

# (8) Bonus: Plot the isotonic regression model.  In particular, plot the
# `predictions` attribute versus the `boundaries` attribute.  You must convert
# each attribute from a Spark `DenseVector` to a NumPy array using the
# `toArray` method.


# ## References

# [Spark Documentation - Classification and regression](https://spark.apache.org/docs/latest/ml-classification-regression.html)

# [Spark Python API - pyspark.ml.feature module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature)

# [Spark Python API - pyspark.ml.regression module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.regression)

# [Spark Python API - pyspark.ml.evaluation module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation)


# ## Stop the SparkSession

spark.stop()
# # Fitting and Evaluating Regression Models - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * A regression algorithm is a supervised learning algorithm.
#   * The inputs are called *features*
#   * The output is called the *label*

# * A regression model provides a prediction of a continuous numerical label.

# * Spark MLlib provides several regression algorithms:
#   * Linear Regression (with Elastic Net, Lasso, and Ridge Regression)
#   * Isotonic Regression
#   * Decision Tree
#   * Random Forest
#   * Gradient-Boosted Trees

# * Spark MLlib also provides regression algorithms for special types of
# continuous numerical labels:
#   * Generalized Regression
#   * Survival Regression

# * Spark MLlib requires the features to be assembled into a vector of doubles column.


# ## Scenario

# We will build a regression model to predict the duration of a ride from
# the distance of the ride.  We can then use this regression model in our
# mobile application to provide a real-time estimate of arrival time.
# In the demonstration, we will use linear regression.  In the exercise, we will
# use isotonic regression.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ## Start a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the (clean) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
rides.printSchema()


# ## Prepare the regression data

# Select the feature and the label and filter out the cancelled rides:
regression_data = rides.select("distance", "duration").filter("cancelled = 0")


# ## Plot the data

# Plot the ride duration versus the ride distance on a random sample of rides
# using pandas:
regression_data \
  .sample(withReplacement=False, fraction=0.1, seed=12345) \
  .toPandas() \
  .plot.scatter(x="distance", y="duration")


# ## Assemble the feature vector

# Import the `VectorAssembler` class from the `pyspark.ml.feature` module:
from pyspark.ml.feature import VectorAssembler

# Create an instance of the `VectorAssembler` class:
assembler = VectorAssembler(inputCols=["distance"], outputCol="features")

# Call the `transform` method to assemble the feature vector:
assembled = assembler.transform(regression_data)

# Examine the transformed DataFrame:
assembled.printSchema()
assembled.show(5)

# **Note:** The `VectorAssembler` instance is an example of a Spark MLlib
# `Transformer`.  It takes a DataFrame as input and returns a DataFrame as
# output via the `transform` method.


# ## Create a train and test set

# Use the `randomSplit` method to create random partitions of the data:
(train, test) = assembled.randomSplit(weights=[0.7, 0.3], seed=23451)

# We will fit the regression model on the `train` DataFrame
# and evaluate it on the `test` DataFrame.


# ## Specify a linear regression model

# Import the `LinearRegression` class from the `pyspark.ml.regression` module:
from pyspark.ml.regression import LinearRegression

# Create an instance of the `LinearRegression` class:
lr = LinearRegression(featuresCol="features", labelCol="duration")

# Examine additional hyperparameters;
print(lr.explainParams())


# ## Fit the linear regression model

# Call the `fit` method to fit the linear regression model on the `train` data:
lr_model = lr.fit(train)

# The `fit` method returns an instance of the `LinearRegressionModel` class:
type(lr_model)

# **Note:** The `LinearRegression` instance is an example of a Spark MLlib
# `Estimator`.  It takes a DataFrame as input and returns a `Transformer` as
# output via the `fit` method.


# ## Examine the model parameters

# Access the `intercept` and `coefficients` attributes to get the intercept and
# slope (for each feature) of the linear regression model:
lr_model.intercept
lr_model.coefficients

# The slope is stored as a `DenseVector`.  Call the `toArray` method to convert
# the DenseVector to a NumPy array:
lr_model.coefficients.toArray()


# ## Examine various model performance measures

# The `summary` attribute is an instance of the `LinearRegressionTrainingSummary` class:
type(lr_model.summary)

# It contains a number of model performance measures:
lr_model.summary.r2
lr_model.summary.rootMeanSquaredError


# ## Examine various model diagnostics

# The `summary` attribute also contains various model diagnostics:
lr_model.summary.coefficientStandardErrors
lr_model.summary.tValues
lr_model.summary.pValues

# **Important:** The first element of each list corresponds to the slope and
# the last element corresponds to the intercept.

# **Note:** The summary attribute contains additional useful information.
  

# ## Apply the linear regression model to the test data

# Use the `transform` method to apply the linear regression model to the `test`
# DataFrame:
predictions = lr_model.transform(test)

# The `transform` method adds a column to the DataFrame with the predicted
# label:
predictions.printSchema()
predictions.show(5)


# ## Evaluate the linear regression model on the test data

# Import the `RegressionEvaluator` class from the `pyspark.ml.evaluation` module:
from pyspark.ml.evaluation import RegressionEvaluator

# Create an instance of the `RegressionEvaluator` class:
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="duration", metricName="r2")

# Call the `explainParams` method to see other metrics:
print(evaluator.explainParams())

# Use the `evaluate` method to compute the metric on the `predictions` DataFrame:
evaluator.evaluate(predictions)

# Use the `setMetricName` method to change the metric:
evaluator.setMetricName("rmse").evaluate(predictions)

# **Note:** You can also use the `evaluate` method of the `LinearRegressionModel` class.


# ## Plot the linear regression model

def plot_lr_model():
  pdf = predictions.sample(withReplacement=False, fraction= 0.1, seed=34512).toPandas()
  plt.scatter("distance", "duration", data=pdf)
  plt.plot("distance", "prediction", color="black", data=pdf)
  plt.xlabel("Distance (m)")
  plt.ylabel("Duration (s)")
  plt.title("Linear Regression Model")
plot_lr_model()


# ## Exercises

# In the following exercises we use *isotonic regression* to fit a monotonic
# function to the data.

# (1)  Import the `IsotonicRegression` class from the regression module.

from pyspark.ml.regression import IsotonicRegression

# (2)  Create an instance of the `IsotonicRegression` class.  Use the same
# features and label that we used for our linear regression model.

ir = IsotonicRegression(featuresCol="features", labelCol="duration")
print(ir.explainParams())

# (3)  Fit the isotonic regression model on the train data.  Note that this
# will produce an instance of the `IsotonicRegressionModel` class.

ir_model = ir.fit(train)
type(ir_model)

# (4)  The model parameters are available in the `boundaries` and `predictions`
# attributes of the isotonic regression model.  Print these attributes.

ir_model.boundaries
ir_model.predictions

# (5) Apply the isotonic regression model to the train data using the
# `transform` method.

predictions_train = ir_model.transform(train)

# (6) Use the `RegressionEvaluator` to compute the RMSE on the train data.

evaluator.evaluate(predictions_train)

# (7) Repeat (5) and (6) on the test data.  Compare the RMSE for the isotonic
# regression model to the RMSE for the linear regression model.

predictions_test = ir_model.transform(test)
evaluator.evaluate(predictions_test)

# (8) Bonus: Plot the isotonic regression model.  In particular, plot the
# `predictions` attribute versus the `boundaries` attribute.  You must convert
# each attribute from a Spark `DenseVector` to a NumPy array using the
# `toArray` method.

def plot_ir_model(predictions):
  pdf = predictions.sample(withReplacement=False, fraction=0.1, seed=34512).toPandas()
  plt.scatter("distance", "duration", data=pdf)
  plt.plot(ir_model.boundaries.toArray(), ir_model.predictions.toArray(), color="black")
  plt.xlabel("Distance (m)")
  plt.ylabel("Duration (s)")
  plt.title("Isotonic Regression Model")
plot_ir_model(predictions_test)


# ## References

# [Spark Documentation - Classification and regression](https://spark.apache.org/docs/latest/ml-classification-regression.html)

# [Spark Python API - pyspark.ml.feature module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature)

# [Spark Python API - pyspark.ml.regression module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.regression)

# [Spark Python API - pyspark.ml.evaluation module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation)


# ## Stop the SparkSession

spark.stop()
# # Fitting and Evaluating Classification Models

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * A classification algorithm is a supervised learning algorithm
#   * The inputs are called *features*
#   * The output is called the *label*

# * A classification model provides a prediction of a categorical label
#   * Binary classification - two categories
#   * Multiclass classification - three or more categories

# * Spark MLlib provides several classification algorithms:
#   * Logistic Regression (with Elastic Net, Lasso, and Ridge Regression)
#   * Decision Tree
#   * Random Forest
#   * Gradient-Boosted Trees
#   * Multilayer Perceptron (Neural Network)
#   * Linear Support Vector Machine (SVM)
#   * Naive Bayes

# * Spark MLlib also provides a meta-algorithm for constructing multiclass
# classification models from binary classification models:
#   * One-vs-Rest

# * Spark MLlib requires the features to be assembled into a vector of doubles column

# * Spark MLlib requires the label to be a zero-based index


# ## Scenario

# In this module we will model the star rating of a ride as a function of
# various attributes of the ride.  Rather than treat the star rating in its
# original form, we will create a binary label that is true if the rating is
# five stars and false otherwise.  We will use [logistic
# regression](https://en.wikipedia.org/wiki/Logistic_regression) to construct a
# binary classification model.  The general workflow will be similar for other
# classification algorithms, although the particular details will vary.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql.functions import col


# ## Start a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )


# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# ## Load the data

# Read the enhanced (joined) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Preprocess the modeling data

# A cancelled ride does not have a star rating.  Use the
# [SQLTransformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.SQLTransformer)
# to filter out the cancelled rides:
from pyspark.ml.feature import SQLTransformer
filterer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0")
filtered = filterer.transform(rides)

# **Note:** `__THIS__` is a placeholder for the DataFrame passed into the `transform` method.


# ## Generate label

# We can treat `star_rating` as a continuous numerical label or an ordered
# categorical label:
filtered.groupBy("star_rating").count().orderBy("star_rating").show()

# Rather than try to predict each value, let us see if we can distinguish
# between five-star and non-five-star ratings.  We can use the
# [Binarizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Binarizer)
# to create our binary label:
from pyspark.ml.feature import Binarizer
converted = filtered.withColumn("star_rating", col("star_rating").cast("double"))
binarizer = Binarizer(inputCol="star_rating", outputCol="high_rating", threshold = 4.5)
labeled = binarizer.transform(converted)
labeled.crosstab("star_rating", "high_rating").show()

# **Note:** `Binarizer` does not like integer values, thus we had to convert to doubles.


# ## Extract, transform, and select features

# Create function to explore features:
def explore(df, feature, label, plot=True):
  from pyspark.sql.functions import count, mean
  aggregated = df.groupby(feature).agg(count(label), mean(label)).orderBy(feature)
  aggregated.show()
  if plot == True:
    pdf = aggregated.toPandas()
    pdf.plot.bar(x=pdf.columns[0], y=pdf.columns[2], capsize=5)

# **Feature 1:** Did the rider review the ride?
engineered1 = labeled.withColumn("reviewed", col("review").isNotNull().cast("int"))
explore(engineered1, "reviewed", "high_rating")

# **Note:** The `avg(high_rating)` gives the observed fraction of a high ratings.

# **Feature 2:** Does the year of the vehicle matter?
explore(labeled, "vehicle_year", "high_rating")

# **Note:** The rider is more likely to give a high rating when the car is
# newer.  We will treat this variable as a continuous feature.

# **Feature 3:** What about the color of the vehicle?
explore(labeled, "vehicle_color", "high_rating")

# **Note:** The rider is more likely to give a high rating if the car is
# black and less likely to give a high rating if the car is yellow.

# The classification algorithms in Spark MLlib do not accept categorical
# features in this form, so let us convert `vehicle_color` to a set of dummy
# variables. First, we use
# [StringIndexer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
# to convert the string codes to numeric codes:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_indexed")
indexer_model = indexer.fit(engineered1)
list(enumerate(indexer_model.labels))
indexed = indexer_model.transform(engineered1)
indexed.select("vehicle_color", "vehicle_color_indexed").show(5)

# Then we use
# [OneHotEncoderEstimator](OneHotEncoderEstimator](https://spark.apache.org/docs/2.4.8/api/python/pyspark.ml.html?highlight=onehotencoderestimator#pyspark.ml.feature.OneHotEncoderEstimator)
# to generate a set of dummy variables:
from pyspark.ml.feature import OneHotEncoderEstimator
encoder = OneHotEncoderEstimator(inputCols=["vehicle_color_indexed"], outputCols=["vehicle_color_encoded"])
encoder_model = encoder.fit(indexed)
encoded = encoder_model.transform(indexed)
encoded.select("vehicle_color", "vehicle_color_indexed", "vehicle_color_encoded").show(5)

# **Note:** `vehicle_color_encoded` is stored as a `SparseVector`.

# Now we can (manually) select our features and label:
selected = encoded.select("reviewed", "vehicle_year", "vehicle_color_encoded", "star_rating", "high_rating")
features = ["reviewed", "vehicle_year", "vehicle_color_encoded"]

# The machine learning algorithms in Spark MLlib expect the features to be
# collected into a single column, so we use
# [VectorAssembler](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)
# to assemble our feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=features, outputCol="features")
assembled = assembler.transform(selected)
assembled.head(5)

# **Note:** `features` is stored as a `SparseVector`.

# Save data for subsequent modules:
assembled.write.parquet(S3_HOME + "/data/modeling_data", mode="overwrite")

# **Note:** We are saving the data to our user directory in HDFS.


# ## Create train and test sets

# We will fit our model on the train DataFrame and evaluate our model on the
# test DataFrame:
(train, test) = assembled.randomSplit([0.7, 0.3], 12345)

# **Important:**  Weights must be doubles.


# ## Specify a logistic regression model

# Use the
# [LogisticRegression](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression)
# class to specify a logistic regression model:
from pyspark.ml.classification import LogisticRegression
log_reg = LogisticRegression(featuresCol="features", labelCol="high_rating")

# Use the `explainParams` method to get a full list of hyperparameters:
print(log_reg.explainParams())


# ## Fit the logistic regression model

# Use the `fit` method to fit the logistic regression model on the train DataFrame:
log_reg_model = log_reg.fit(train)

# The result is an instance of the
# [LogisticRegressionModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegressionModel)
# class:
type(log_reg_model)


# ## Examine the logistic regression model

# The model parameters are stored in the `intercept` and `coefficients` attributes:
log_reg_model.intercept
log_reg_model.coefficients

# The `summary` attribute is an instance of the
# [BinaryLogisticRegressionTrainingSummary](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.BinaryLogisticRegressionTrainingSummary)
# class:
type(log_reg_model.summary)

# We can query the iteration history:
log_reg_model.summary.totalIterations
log_reg_model.summary.objectiveHistory

# and plot it too:
def plot_iterations(summary):
  plt.plot(summary.objectiveHistory)
  plt.title("Training Summary")
  plt.xlabel("Iteration")
  plt.ylabel("Objective Function")
  plt.show()

plot_iterations(log_reg_model.summary)

# We can also query the model performance, in this case, the area under the ROC curve:
log_reg_model.summary.areaUnderROC

# and plot the ROC curve:
log_reg_model.summary.roc.show(5)

def plot_roc_curve(summary):
  roc_curve = summary.roc.toPandas()
  plt.plot(roc_curve["FPR"], roc_curve["FPR"], "k")
  plt.plot(roc_curve["FPR"], roc_curve["TPR"])
  plt.title("ROC Area: %s" % summary.areaUnderROC)
  plt.xlabel("False Positive Rate")
  plt.ylabel("True Positive Rate")
  plt.show()

plot_roc_curve(log_reg_model.summary)


# ## Evaluate model performance on the test set

# We have been assessing the model performance on the train DataFrame.  We
# really want to assess it on the test DataFrame.

# **Method 1:** Use the `evaluate` method of the `LogisticRegressionModel` class

test_summary = log_reg_model.evaluate(test)

# The result is an instance of the
# [BinaryLogisticRegressionSummary](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.BinaryLogisticRegressionSummary)
# class:
type(test_summary)

# It has attributes similar to those of the
# `BinaryLogisticRegressionTrainingSummary` class:
test_summary.areaUnderROC
plot_roc_curve(test_summary)

# **Method 2:** Use the `evaluate` method of the
# [BinaryClassificationEvaluator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.BinaryClassificationEvaluator)
# class

# Generate predictions on the test DataFrame:
test_with_prediction = log_reg_model.transform(test)
test_with_prediction.show(5)

# **Note:** The resulting DataFrame includes three types of predictions.  The
# `rawPrediction` is a vector of log-odds, `prediction` is a vector or
# probabilities `prediction` is the predicted class based on the probability
# vector.

# Create an instance of `BinaryClassificationEvaluator` class:
from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="high_rating", metricName="areaUnderROC")
print(evaluator.explainParams())
evaluator.evaluate(test_with_prediction)

# Evaluate using another metric:
evaluator.setMetricName("areaUnderPR").evaluate(test_with_prediction)


# ## Exercises

# In the exercises we add another feature to the classification model and
# determine if it improves the model performance.

# (1) Consider the `encoded` DataFrame.  Use the `explore` function to
# determine if `vehicle_noir` is a promising feature.

# (2) Reassemble the feature vector and include `vehicle_noir`.

# (3) Create new train and test datasets.

# (4) Refit the logistic regression model on the train dataset.

# (5) Apply the refit logistic model to the test dataset.

# (6) Compute the AUC on the test dataset.

# (7) We committed a cardinal sin of machine learning above.  What was it?


# ## References

# [Spark Documentation - Classification and regression](https://spark.apache.org/docs/latest/ml-classification-regression.html)

# [Spark Python API - pyspark.ml.feature module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature)

# [Spark Python API - pyspark.ml.classification module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.classification)

# [Spark Python API - pyspark.ml.evaluation module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation)


# ## Stop the SparkSession 

spark.stop()
# # Fitting and Evaluating Classification Models - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * A classification algorithm is a supervised learning algorithm
#   * The inputs are called *features*
#   * The output is called the *label*

# * A classification model provides a prediction of a categorical label
#   * Binary classification - two categories
#   * Multiclass classification - three or more categories

# * Spark MLlib provides several classification algorithms:
#   * Logistic Regression (with Elastic Net, Lasso, and Ridge Regression)
#   * Decision Tree
#   * Random Forest
#   * Gradient-Boosted Trees
#   * Multilayer Perceptron (Neural Network)
#   * Linear Support Vector Machine (SVM)
#   * Naive Bayes

# * Spark MLlib also provides a meta-algorithm for constructing multiclass
# classification models from binary classification models:
#   * One-vs-Rest

# * Spark MLlib requires the features to be assembled into a vector of doubles column

# * Spark MLlib requires the label to be a zero-based index


# ## Scenario

# In this module we will model the star rating of a ride as a function of
# various attributes of the ride.  Rather than treat the star rating in its
# original form, we will create a binary label that is true if the rating is
# five stars and false otherwise.  We will use [logistic
# regression](https://en.wikipedia.org/wiki/Logistic_regression) to construct a
# binary classification model.  The general workflow will be similar for other
# classification algorithms, although the particular details will vary.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql.functions import col


# ## Start a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )


# ## Load the data

# Read the enhanced (joined) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Preprocess the modeling data

# A cancelled ride does not have a star rating.  Use the
# [SQLTransformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.SQLTransformer)
# to filter out the cancelled rides:
from pyspark.ml.feature import SQLTransformer
filterer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0")
filtered = filterer.transform(rides)

# **Note:** `__THIS__` is a placeholder for the DataFrame passed into the `transform` method.


# ## Generate label

# We can treat `star_rating` as a continuous numerical label or an ordered
# categorical label:
filtered.groupBy("star_rating").count().orderBy("star_rating").show()

# Rather than try to predict each value, let us see if we can distinguish
# between five-star and non-five-star ratings.  We can use the
# [Binarizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Binarizer)
# to create our binary label:
from pyspark.ml.feature import Binarizer
converted = filtered.withColumn("star_rating", col("star_rating").cast("double"))
binarizer = Binarizer(inputCol="star_rating", outputCol="high_rating", threshold = 4.5)
labeled = binarizer.transform(converted)
labeled.crosstab("star_rating", "high_rating").show()

# **Note:** `Binarizer` does not like integer values, thus we had to convert to doubles.


# ## Extract, transform, and select features

# Create function to explore features:
def explore(df, feature, label, plot=True):
  from pyspark.sql.functions import count, mean
  aggregated = df.groupby(feature).agg(count(label), mean(label)).orderBy(feature)
  aggregated.show()
  if plot == True:
    pdf = aggregated.toPandas()
    pdf.plot.bar(x=pdf.columns[0], y=pdf.columns[2], capsize=5)

# **Feature 1:** Did the rider review the ride?
engineered1 = labeled.withColumn("reviewed", col("review").isNotNull().cast("int"))
explore(engineered1, "reviewed", "high_rating")

# **Note:** The `avg(high_rating)` gives the observed fraction of a high ratings.

# **Feature 2:** Does the year of the vehicle matter?
explore(labeled, "vehicle_year", "high_rating")

# **Note:** The rider is more likely to give a high rating when the car is
# newer.  We will treat this variable as a continuous feature.

# **Feature 3:** What about the color of the vehicle?
explore(labeled, "vehicle_color", "high_rating")

# **Note:** The rider is more likely to give a high rating if the car is
# black and less likely to give a high rating if the car is yellow.

# The classification algorithms in Spark MLlib do not accept categorical
# features in this form, so let us convert `vehicle_color` to a set of dummy
# variables. First, we use
# [StringIndexer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
# to convert the string codes to numeric codes:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_indexed")
indexer_model = indexer.fit(engineered1)
list(enumerate(indexer_model.labels))
indexed = indexer_model.transform(engineered1)
indexed.select("vehicle_color", "vehicle_color_indexed").show(5)

# Then we use
# [OneHotEncoderEstimator](OneHotEncoderEstimator](https://spark.apache.org/docs/2.4.8/api/python/pyspark.ml.html?highlight=onehotencoderestimator#pyspark.ml.feature.OneHotEncoderEstimator)
# to generate a set of dummy variables:
from pyspark.ml.feature import OneHotEncoderEstimator
encoder = OneHotEncoderEstimator(inputCols=["vehicle_color_indexed"], outputCols=["vehicle_color_encoded"])
encoder_model = encoder.fit(indexed)
encoded = encoder_model.transform(indexed)
encoded.select("vehicle_color", "vehicle_color_indexed", "vehicle_color_encoded").show(5)

# **Note:** `vehicle_color_encoded` is stored as a `SparseVector`.

# Now we can (manually) select our features and label:
selected = encoded.select("reviewed", "vehicle_year", "vehicle_color_encoded", "star_rating", "high_rating")
features = ["reviewed", "vehicle_year", "vehicle_color_encoded"]

# The machine learning algorithms in Spark MLlib expect the features to be
# collected into a single column, so we use
# [VectorAssembler](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)
# to assemble our feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=features, outputCol="features")
assembled = assembler.transform(selected)
assembled.head(5)

# **Note:** `features` is stored as a `SparseVector`.

# Save data for subsequent modules:
assembled.write.parquet(S3_HOME + "/data/modeling_data", mode="overwrite")

# **Note:** We are saving the data to our user directory in HDFS.


# ## Create train and test sets

# We will fit our model on the train DataFrame and evaluate our model on the
# test DataFrame:
(train, test) = assembled.randomSplit([0.7, 0.3], 12345)

# **Important:**  Weights must be doubles.


# ## Specify a logistic regression model

# Use the
# [LogisticRegression](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression)
# class to specify a logistic regression model:
from pyspark.ml.classification import LogisticRegression
log_reg = LogisticRegression(featuresCol="features", labelCol="high_rating")

# Use the `explainParams` method to get a full list of hyperparameters:
print(log_reg.explainParams())


# ## Fit the logistic regression model

# Use the `fit` method to fit the logistic regression model on the train DataFrame:
log_reg_model = log_reg.fit(train)

# The result is an instance of the
# [LogisticRegressionModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegressionModel)
# class:
type(log_reg_model)


# ## Examine the logistic regression model

# The model parameters are stored in the `intercept` and `coefficients` attributes:
log_reg_model.intercept
log_reg_model.coefficients

# The `summary` attribute is an instance of the
# [BinaryLogisticRegressionTrainingSummary](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.BinaryLogisticRegressionTrainingSummary)
# class:
type(log_reg_model.summary)

# We can query the iteration history:
log_reg_model.summary.totalIterations
log_reg_model.summary.objectiveHistory

# and plot it too:
def plot_iterations(summary):
  plt.plot(summary.objectiveHistory)
  plt.title("Training Summary")
  plt.xlabel("Iteration")
  plt.ylabel("Objective Function")
  plt.show()

plot_iterations(log_reg_model.summary)

# We can also query the model performance, in this case, the area under the ROC curve:
log_reg_model.summary.areaUnderROC

# and plot the ROC curve:
log_reg_model.summary.roc.show(5)

def plot_roc_curve(summary):
  roc_curve = summary.roc.toPandas()
  plt.plot(roc_curve["FPR"], roc_curve["FPR"], "k")
  plt.plot(roc_curve["FPR"], roc_curve["TPR"])
  plt.title("ROC Area: %s" % summary.areaUnderROC)
  plt.xlabel("False Positive Rate")
  plt.ylabel("True Positive Rate")
  plt.show()

plot_roc_curve(log_reg_model.summary)


# ## Evaluate model performance on the test set

# We have been assessing the model performance on the train DataFrame.  We
# really want to assess it on the test DataFrame.

# **Method 1:** Use the `evaluate` method of the `LogisticRegressionModel` class

test_summary = log_reg_model.evaluate(test)

# The result is an instance of the
# [BinaryLogisticRegressionSummary](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.BinaryLogisticRegressionSummary)
# class:
type(test_summary)

# It has attributes similar to those of the
# `BinaryLogisticRegressionTrainingSummary` class:
test_summary.areaUnderROC
plot_roc_curve(test_summary)

# **Method 2:** Use the `evaluate` method of the
# [BinaryClassificationEvaluator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.BinaryClassificationEvaluator)
# class

# Generate predictions on the test DataFrame:
test_with_prediction = log_reg_model.transform(test)
test_with_prediction.show(5)

# **Note:** The resulting DataFrame includes three types of predictions.  The
# `rawPrediction` is a vector of log-odds, `prediction` is a vector or
# probabilities `prediction` is the predicted class based on the probability
# vector.

# Create an instance of `BinaryClassificationEvaluator` class:
from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="high_rating", metricName="areaUnderROC")
print(evaluator.explainParams())
evaluator.evaluate(test_with_prediction)

# Evaluate using another metric:
evaluator.setMetricName("areaUnderPR").evaluate(test_with_prediction)


# ## Exercises

# In the exercises we add another feature to the classification model and
# determine if it improves the model performance.

# (1) Consider the `encoded` DataFrame.  Use the `explore` function to
# determine if `vehicle_noir` is a promising feature.

explore(encoded, "vehicle_noir", "high_rating")

# (2) Reassemble the feature vector and include `vehicle_noir`.

features = ["reviewed", "vehicle_year", "vehicle_color_encoded", "vehicle_noir"]
assembler = VectorAssembler(inputCols=features, outputCol="features")
assembled = assembler.transform(encoded)

# (3) Create new train and test datasets.

(train, test) = assembled.randomSplit([0.7, 0.3], 23451)

# (4) Refit the logistic regression model on the train dataset.

log_reg_model = log_reg.fit(train)

# (5) Apply the refit logistic model to the test dataset.

predictions = log_reg_model.transform(test)

# (6) Compute the AUC on the test dataset.

evaluator.setMetricName("areaUnderROC").evaluate(predictions)

# (7) We committed a cardinal sin of machine learning above.  What was it?

# We assessed the potential of `vehicle_noir` on all the data, which includes
# our test dataset.  This compromises the integrity of the test dataset.  Spark
# MLlib Pipelines will allow us to address this situation.


# ## References

# [Spark Documentation - Classification and regression](https://spark.apache.org/docs/latest/ml-classification-regression.html)

# [Spark Python API - pyspark.ml.feature module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature)

# [Spark Python API - pyspark.ml.classification module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.classification)

# [Spark Python API - pyspark.ml.evaluation module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation)


# ## Stop the SparkSession 

spark.stop()
# # Tuning Algorithm Hyperparameters Using Grid Search

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# Most machine learning algorithms have a set of user-specified parameters that
# govern the behavior of the algorithm.  These parameters are called
# *hyperparameters* to distinguish them from the model parameters such as the
# intercept and coefficients in linear and logistic regression.  In this module
# we show how to use grid search and cross validation in Spark MLlib to
# determine a reasonable regularization parameter for [$l1$ lasso linear
# regression](https://en.wikipedia.org/wiki/Lasso_%28statistics%29).


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# **Important:**  Run the `15_classify.py` script before loading the data.

# Read the modeling data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/data/modeling_data")
rides.show(5)


# ## Create train and test data

(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Requirements for hyperparameter tuning

# We need to specify four components to perform hyperparameter tuning using
# grid search:
# * Estimator (i.e. machine learning algorithm)
# * Hyperparameter grid
# * Evaluator
# * Validation method


# ## Specify the estimator

# In this example we will use $l1$ (lasso) linear regression as our estimator:
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="star_rating", elasticNetParam=1.0)

# Use the `explainParams` method to get the full list of hyperparameters:
print(lr.explainParams())

# Setting `elasticNetParam=1.0` corresponds to $l1$ (lasso) linear regression.
# We are interested in finding a reasonable value of `regParam`.


# ## Specify the hyperparameter grid

# Use the
# [ParamGridBuilder](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder)
# class to specify a hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
regParamList = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
grid = ParamGridBuilder().addGrid(lr.regParam, regParamList).build()

# The resulting object is simply a list of parameter maps:
grid

# Rather than specify `elasticNetParam` in the `LinearRegression` constructor, we can specify it in our grid:
grid = ParamGridBuilder().baseOn({lr.elasticNetParam: 1.0}).addGrid(lr.regParam, regParamList).build()
grid


# ## Specify the evaluator

# In this case we will use
# [RegressionEvaluator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.RegressionEvaluator)
# as our evaluator and specify root-mean-squared error as the metric:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="star_rating", metricName="rmse")


# ## Tune the hyperparameters using holdout cross-validation

# In most cases, holdout cross-validation will be sufficient.  Use the
# [TrainValidationSplit](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplit)
# class to specify holdout cross-validation:
from pyspark.ml.tuning import TrainValidationSplit
tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, trainRatio=0.75, seed=54321)

# For each combination of the hyperparameters, the linear regression will be
# fit on a random %75 of the `train` DataFrame and evaluated on the remaining
# %25. 

# Use the `fit` method to find the best set of hyperparameters:
%time tvs_model = tvs.fit(train)

# The resulting model is an instance of the
# [TrainValidationSplitModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplitModel)
# class:
type(tvs_model)

# The cross-validation results are stored in the `validationMetrics` attribute:
tvs_model.validationMetrics

# These are the RMSE for each set of hyperparameters.  Smaller is better.

def plot_holdout_results(model):
  plt.plot(regParamList, model.validationMetrics)
  plt.title("Hyperparameter Tuning Results")
  plt.xlabel("Regularization Parameter")
  plt.ylabel("Validation Metric")
  plt.show()
plot_holdout_results(tvs_model)

# In this case the `bestModel` attribute is an instance of the
# [LinearRegressionModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.LinearRegressionModel)
# class:
type(tvs_model.bestModel)

# **Note:** The model is rerun on the entire train dataset using the best set of hyperparameters.

# The usual attributes and methods are available:
tvs_model.bestModel.intercept
tvs_model.bestModel.coefficients
tvs_model.bestModel.summary.rootMeanSquaredError
tvs_model.bestModel.evaluate(test).r2


# ## Tune the hyperparameters using k-fold cross-validation

# For small or noisy datasets, k-fold cross-validation may be more appropriate.
# Use the
# [CrossValidator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator)
# class to specify the k-fold cross-validation:
from pyspark.ml.tuning import CrossValidator
cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3, seed=54321)
%time cv_model = cv.fit(train)

# The result is an instance of the
# [CrossValidatorModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidatorModel)
# class:
type(cv_model)

# The cross-validation results are stored in the `avgMetrics` attribute:
cv_model.avgMetrics
def plot_kfold_results(model):
  plt.plot(regParamList, model.avgMetrics)
  plt.title("Hyperparameter Tuning Results")
  plt.xlabel("Regularization Parameter")
  plt.ylabel("Validation Metric")
  plt.show()
plot_kfold_results(cv_model)

# The `bestModel` attribute contains the model based on the best set of
# hyperparameters.  In this case, it is an instance of the
# `LinearRegressionModel` class:
type(cv_model.bestModel)

# Compute the performance of the performance of the best model on the test
# dataset:
cv_model.bestModel.evaluate(test).r2


# ## Exercises

# (1) Maybe our regularization parameters are too large.  Rerun the
# hyperparameter tuning with regularization parameters [0.0, 0.002, 0.004, 0.006,
# 0.008, 0.01].

# (2) Create a parameter grid that searches over `elasticNetParam` as well as
# `regParam`.


# ## References

# [Spark Documentation - Model Selection and hyperparameter
# tuning](http://spark.apache.org/docs/latest/ml-tuning.html)

# [Spark Python API - pyspark.ml.tuning
# module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.tuning)


# ## Stop the SparkSession

spark.stop()

# # Tuning Algorithm Hyperparameters Using Grid Search - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# Most machine learning algorithms have a set of user-specified parameters that
# govern the behavior of the algorithm.  These parameters are called
# *hyperparameters* to distinguish them from the model parameters such as the
# intercept and coefficients in linear and logistic regression.  In this module
# we show how to use grid search and cross validation in Spark MLlib to
# determine a reasonable regularization parameter for [$l1$ lasso linear
# regression](https://en.wikipedia.org/wiki/Lasso_%28statistics%29).


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )


# ## Load the data

# **Important:**  Run the `15_classify.py` script before loading the data.

# Read the modeling data from HDFS:
rides = spark.read.parquet(S3_HOME + "/data/modeling_data")
rides.show(5)


# ## Create train and test data

(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Requirements for hyperparameter tuning

# We need to specify four components to perform hyperparameter tuning using
# grid search:
# * Estimator (i.e. machine learning algorithm)
# * Hyperparameter grid
# * Evaluator
# * Validation method


# ## Specify the estimator

# In this example we will use $l1$ (lasso) linear regression as our estimator:
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="star_rating", elasticNetParam=1.0)

# Use the `explainParams` method to get the full list of hyperparameters:
print(lr.explainParams())

# Setting `elasticNetParam=1.0` corresponds to $l1$ (lasso) linear regression.
# We are interested in finding a reasonable value of `regParam`.


# ## Specify the hyperparameter grid

# Use the
# [ParamGridBuilder](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder)
# class to specify a hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
regParamList = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
grid = ParamGridBuilder().addGrid(lr.regParam, regParamList).build()

# The resulting object is simply a list of parameter maps:
grid

# Rather than specify `elasticNetParam` in the `LinearRegression` constructor, we can specify it in our grid:
grid = ParamGridBuilder().baseOn({lr.elasticNetParam: 1.0}).addGrid(lr.regParam, regParamList).build()
grid


# ## Specify the evaluator

# In this case we will use
# [RegressionEvaluator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.RegressionEvaluator)
# as our evaluator and specify root-mean-squared error as the metric:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="star_rating", metricName="rmse")


# ## Tune the hyperparameters using holdout cross-validation

# In most cases, holdout cross-validation will be sufficient.  Use the
# [TrainValidationSplit](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplit)
# class to specify holdout cross-validation:
from pyspark.ml.tuning import TrainValidationSplit
tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, trainRatio=0.75, seed=54321)

# For each combination of the hyperparameters, the linear regression will be
# fit on a random %75 of the `train` DataFrame and evaluated on the remaining
# %25. 

# Use the `fit` method to find the best set of hyperparameters:
%time tvs_model = tvs.fit(train)

# The resulting model is an instance of the
# [TrainValidationSplitModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplitModel)
# class:
type(tvs_model)

# The cross-validation results are stored in the `validationMetrics` attribute:
tvs_model.validationMetrics

# These are the RMSE for each set of hyperparameters.  Smaller is better.

def plot_holdout_results(model):
  plt.plot(regParamList, model.validationMetrics)
  plt.title("Hyperparameter Tuning Results")
  plt.xlabel("Regularization Parameter")
  plt.ylabel("Validation Metric")
  plt.show()
plot_holdout_results(tvs_model)

# In this case the `bestModel` attribute is an instance of the
# [LinearRegressionModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.LinearRegressionModel)
# class:
type(tvs_model.bestModel)

# **Note:** The model is rerun on the entire train dataset using the best set of hyperparameters.

# The usual attributes and methods are available:
tvs_model.bestModel.intercept
tvs_model.bestModel.coefficients
tvs_model.bestModel.summary.rootMeanSquaredError
tvs_model.bestModel.evaluate(test).r2


# ## Tune the hyperparameters using k-fold cross-validation

# For small or noisy datasets, k-fold cross-validation may be more appropriate.
# Use the
# [CrossValidator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator)
# class to specify the k-fold cross-validation:
from pyspark.ml.tuning import CrossValidator
cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3, seed=54321)
%time cv_model = cv.fit(train)

# The result is an instance of the
# [CrossValidatorModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidatorModel)
# class:
type(cv_model)

# The cross-validation results are stored in the `avgMetrics` attribute:
cv_model.avgMetrics
def plot_kfold_results(model):
  plt.plot(regParamList, model.avgMetrics)
  plt.title("Hyperparameter Tuning Results")
  plt.xlabel("Regularization Parameter")
  plt.ylabel("Validation Metric")
  plt.show()
plot_kfold_results(cv_model)

# The `bestModel` attribute contains the model based on the best set of
# hyperparameters.  In this case, it is an instance of the
# `LinearRegressionModel` class:
type(cv_model.bestModel)

# Compute the performance of the performance of the best model on the test
# dataset:
cv_model.bestModel.evaluate(test).r2


# ## Exercises

# (1) Maybe our regularization parameters are too large.  Rerun the
# hyperparameter tuning with regularization parameters [0.0, 0.002, 0.004, 0.006,
# 0.008, 0.01].

regParamList = [0.0, 0.002, 0.004, 0.006, 0.008, 0.01]
grid2 = ParamGridBuilder().addGrid(lr.regParam, regParamList).build()

tvs2 = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid2, evaluator=evaluator, trainRatio=0.75, seed=54321)
%time tvs_model2 = tvs2.fit(train)

plot_holdout_results(tvs_model2)

tvs_model2.bestModel.intercept
tvs_model2.bestModel.coefficients

tvs_model2.bestModel.evaluate(test).r2
tvs_model2.bestModel.evaluate(test).rootMeanSquaredError

# (2) Create a parameter grid that searches over `elasticNetParam` as well as
# `regParam`.

elasticNetParamList = [0.0, 0.5, 1.0]
regParamList = [0.0, 0.002, 0.004, 0.006, 0.008, 0.01]
grid3 = ParamGridBuilder() \
  .addGrid(lr.elasticNetParam, elasticNetParamList) \
  .addGrid(lr.regParam, regParamList) \
  .build()

tvs3 = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid3, evaluator=evaluator, trainRatio=0.75, seed=54321)
%time tvs_model3 = tvs3.fit(train)

tvs_model3.bestModel.intercept
tvs_model3.bestModel.coefficients

tvs_model3.bestModel.evaluate(test).r2
tvs_model3.bestModel.evaluate(test).rootMeanSquaredError


# ## References

# [Spark Documentation - Model Selection and hyperparameter
# tuning](http://spark.apache.org/docs/latest/ml-tuning.html)

# [Spark Python API - pyspark.ml.tuning
# module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.tuning)


# ## Stop the SparkSession

spark.stop()

# # Fitting and Evaluating Clustering Models

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * Clustering algorithms are unsupervised learning algorithms

# * A clustering model groups observations that are similar in some sense based
# on a feature vector

# * The number of clusters is a hyperparameter

# * A common use case for clustering is customer segmentation

# * Clustering is as much an art as a science

# * Spark MLlib provides a few clustering algorithms:
#   * K-means
#   * Bisecting K-means
#   * Gaussian mixture model
#   * Latent Dirichlet allocation


# ## Scenario

# In this demonstration we use a Gaussian mixture model to cluster the student
# riders by their home latitude and longitude.


# ## Setup

# Import useful packages, modules, classes, and functions:

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium

from pyspark.sql.functions import col


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the clean rider data from HDFS:
riders = spark.read.parquet(S3_ROOT + "/duocar/clean/riders/")


# ## Preprocess the data

# Select the student riders:
students = riders.filter(col("student") == True)


# ## Print and plot the home coordinates

# Print the home coordinates:
students.select("home_lat", "home_lon").show(10)

# Plot the home coordinates:
def plot_data(df):
  # Create a map of Fargo, North Dakota, USA:
  m = folium.Map(location=[46.8772222, -96.7894444], zoom_start=13)
  # Add the home coordinates:
  rows = df.select("home_lat", "home_lon").collect()
  for row in rows:
    folium.CircleMarker(location=[row["home_lat"], row["home_lon"]], radius=2, fill=True).add_to(m)
  # Return the map:
  return(m)
plot_data(students)


# ## Extract, transform, and select the features

# We would normally scale our features to have the same units using a feature
# transformation such as the `StandardScaler`.  However, since latitude and
# longitude are already in similar scales, we can proceed.

# Select home latitude and longitude as the features:
selected = ["home_lat", "home_lon"]

# Assemble the feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=selected, outputCol="features")
assembled = assembler.transform(students)


# ## Fit a Gaussian mixture model

# Specify a Gaussian mixture model with two clusters:
from pyspark.ml.clustering import GaussianMixture
gm = GaussianMixture(featuresCol="features", k=2, seed=12345)

# Examine the hyperparameters:
print(gm.explainParams())

# Fit the Gaussian mixture model:
gmm = gm.fit(assembled)
type(gmm)


# ## Examine the Gaussian mixture model

# Examine the mixing weights:
gmm.weights

# Examine the (multivariate) Gaussian distributions:
gmm.gaussiansDF.head(5)

# Examine the model summary:
gmm.hasSummary

# Examine the cluster sizes:
gmm.summary.clusterSizes

# Examine the predictions DataFrame:
gmm.summary.predictions.printSchema()
gmm.summary.predictions.select("features", "prediction", "probability").head(10)


# ## Evaluate the Gaussian mixture model

# Extract the log-likelihood from the summary object:
gmm.summary.logLikelihood

# Use the `ClusteringEvaluator` to compute the [silhouette
# measure](https://en.wikipedia.org/wiki/Silhouette_%28clustering%29):
from pyspark.ml.evaluation import ClusteringEvaluator
evaluator = ClusteringEvaluator()
evaluator.evaluate(gmm.summary.predictions)


# ## Plot the clusters

def plot_clusters(gmm):
  # Specify a color palette:
  colors = ["blue", "orange", "green", "red"]
  # Create a map of Fargo, North Dakota, USA:
  m = folium.Map(location=[46.8772222, -96.7894444], zoom_start=13)
  # Add the home coordinates:
  rows = gmm.summary.predictions.select("home_lat", "home_lon", "prediction").collect()
  for row in rows:
    folium.CircleMarker(location=[row["home_lat"], row["home_lon"]], radius=2, color=colors[row["prediction"]], fill=True).add_to(m)
  # Add the cluster centers (Gaussian means):
  centers = gmm.gaussiansDF.collect()
  for (i, center) in enumerate(centers):
    folium.CircleMarker(location=center["mean"], color=colors[i]).add_to(m)
  # Return the map:
  return(m)
plot_clusters(gmm)


# ## Explore the cluster profiles

# Print the distribution of `gender` by cluster:
gmm.summary.predictions \
  .groupBy("prediction", "gender") \
  .count() \
  .orderBy("prediction", "gender") \
  .show()

# Plot the distribution of `gender` by cluster:

pdf = gmm.summary.predictions.fillna("missing", subset=["gender"]).toPandas()
sns.countplot(data=pdf, x="prediction", hue="gender", hue_order=["female", "male", "missing"])


# ## Save and load the Gaussian mixture model

# Save the Gaussian mixture model to HDFS and overwrite any existing directory:
gmm.write().overwrite().save(S3_HOME + "/models/gmm")

# The following shortcut will save the model and return an error if the directory exists:
#```python
#gmm.save(S3_HOME + "/models/gmm")
#```

# Load the Gaussian mixture model from HDFS:
from pyspark.ml.clustering import GaussianMixtureModel
gmm_loaded = GaussianMixtureModel.read().load(S3_HOME + "/models/gmm")

# Alternatively, use the following convenience method:
#```python
#gmm_loaded = GaussianMixtureModel.load(S3_HOME + "/models/gmm")
#```


# Apply the loaded Gaussian mixture model:
clustered = gmm_loaded.transform(assembled)
clustered.printSchema()


# ## Exercises

# (1) Specify a Gaussian mixture model with three clusters.

# (2) Fit the Gaussian mixture model on the `assembled` DataFrame.

# (3) Examine the Gaussian mixture model parameters stored in
# the `weights` and `GaussiansDF` attributes.

# (4) Plot the Gaussian mixture model using the `plot_clusters` function.

# (5) Apply the Gaussian mixture model to the `assembled` DataFrame using the `transform` method:

# (6) Print the distribution of `gender` by cluster.

# (7) **Bonus:** Plot the distribution of `gender` by cluster.


# ## References

# [Wikipedia - Cluster analysis](https://en.wikipedia.org/wiki/Cluster_analysis)

# [Spark Documentation - Clustering](http://spark.apache.org/docs/latest/ml-clustering.html)

# [Spark Python API - pyspark.ml.clustering.GaussianMixture class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixture)

# [Spark Python API - pyspark.ml.clustering.GaussianMixtureModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixtureModel)

# [Spark Python API - pyspark.ml.clustering.GaussianMixtureSummary class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixtureSummary)


# ## Stop the SparkSession

spark.stop()
# # Fitting and Evaluating Clustering Models - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * Clustering algorithms are unsupervised learning algorithms

# * A clustering model groups observations that are similar in some sense based
# on a feature vector

# * The number of clusters is a hyperparameter

# * A common use case for clustering is customer segmentation

# * Clustering is as much an art as a science

# * Spark MLlib provides a few clustering algorithms:
#   * K-means
#   * Bisecting K-means
#   * Gaussian mixture model
#   * Latent Dirichlet allocation


# ## Scenario

# In this demonstration we use a Gaussian mixture model to cluster the student
# riders by their home latitude and longitude.


# ## Setup

# Import useful packages, modules, classes, and functions:

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium

from pyspark.sql.functions import col


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the clean rider data from HDFS:
riders = spark.read.parquet(S3_ROOT + "/duocar/clean/riders/")


# ## Preprocess the data

# Select the student riders:
students = riders.filter(col("student") == True)


# ## Print and plot the home coordinates

# Print the home coordinates:
students.select("home_lat", "home_lon").show(10)

# Plot the home coordinates:
def plot_data(df):
  # Create a map of Fargo, North Dakota, USA:
  m = folium.Map(location=[46.8772222, -96.7894444], zoom_start=13)
  # Add the home coordinates:
  rows = df.select("home_lat", "home_lon").collect()
  for row in rows:
    folium.CircleMarker(location=[row["home_lat"], row["home_lon"]], radius=2, fill=True).add_to(m)
  # Return the map:
  return(m)
plot_data(students)


# ## Extract, transform, and select the features

# We would normally scale our features to have the same units using a feature
# transformation such as the `StandardScaler`.  However, since latitude and
# longitude are already in similar scales, we can proceed.

# Select home latitude and longitude as the features:
selected = ["home_lat", "home_lon"]

# Assemble the feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=selected, outputCol="features")
assembled = assembler.transform(students)


# ## Fit a Gaussian mixture model

# Specify a Gaussian mixture model with two clusters:
from pyspark.ml.clustering import GaussianMixture
gm = GaussianMixture(featuresCol="features", k=2, seed=12345)

# Examine the hyperparameters:
print(gm.explainParams())

# Fit the Gaussian mixture model:
gmm = gm.fit(assembled)
type(gmm)


# ## Examine the Gaussian mixture model

# Examine the mixing weights:
gmm.weights

# Examine the (multivariate) Gaussian distributions:
gmm.gaussiansDF.head(5)

# Examine the model summary:
gmm.hasSummary

# Examine the cluster sizes:
gmm.summary.clusterSizes

# Examine the predictions DataFrame:
gmm.summary.predictions.printSchema()
gmm.summary.predictions.select("features", "prediction", "probability").head(10)


# ## Evaluate the Gaussian mixture model

# Extract the log-likelihood from the summary object:
gmm.summary.logLikelihood

# Use the `ClusteringEvaluator` to compute the [silhouette
# measure](https://en.wikipedia.org/wiki/Silhouette_%28clustering%29):
from pyspark.ml.evaluation import ClusteringEvaluator
evaluator = ClusteringEvaluator()
evaluator.evaluate(gmm.summary.predictions)


# ## Plot the clusters

def plot_clusters(gmm):
  # Specify a color palette:
  colors = ["blue", "orange", "green", "red"]
  # Create a map of Fargo, North Dakota, USA:
  m = folium.Map(location=[46.8772222, -96.7894444], zoom_start=13)
  # Add the home coordinates:
  rows = gmm.summary.predictions.select("home_lat", "home_lon", "prediction").collect()
  for row in rows:
    folium.CircleMarker(location=[row["home_lat"], row["home_lon"]], radius=2, color=colors[row["prediction"]], fill=True).add_to(m)
  # Add the cluster centers (Gaussian means):
  centers = gmm.gaussiansDF.collect()
  for (i, center) in enumerate(centers):
    folium.CircleMarker(location=center["mean"], color=colors[i]).add_to(m)
  # Return the map:
  return(m)
plot_clusters(gmm)


# ## Explore the cluster profiles

# Print the distribution of `gender` by cluster:
gmm.summary.predictions \
  .groupBy("prediction", "gender") \
  .count() \
  .orderBy("prediction", "gender") \
  .show()

# Plot the distribution of `gender` by cluster:

pdf = gmm.summary.predictions.fillna("missing", subset=["gender"]).toPandas()
sns.countplot(data=pdf, x="prediction", hue="gender", hue_order=["female", "male", "missing"])


# ## Save and load the Gaussian mixture model

# Save the Gaussian mixture model to HDFS and overwrite any existing directory:
gmm.write().overwrite().save(S3_HOME + "/models/gmm")

# The following shortcut will save the model and return an error if the directory exists:
#```python
#gmm.save("models/gmm")
#```

# Load the Gaussian mixture model from HDFS:
from pyspark.ml.clustering import GaussianMixtureModel
gmm_loaded = GaussianMixtureModel.read().load(S3_HOME + "/models/gmm")

# Alternatively, use the following convenience method:
#```python
#gmm_loaded = GaussianMixtureModel.load("models/gmm")
#```


# Apply the loaded Gaussian mixture model:
clustered = gmm_loaded.transform(assembled)
clustered.printSchema()


# ## Exercises

# (1) Specify a Gaussian mixture model with three clusters.

gm3 = GaussianMixture(featuresCol="features", k=3, seed=12345)

# (2) Fit the Gaussian mixture model on the `assembled` DataFrame.

gmm3 = gm3.fit(assembled)

# (3) Examine the Gaussian mixture model parameters stored in
# the `weights` and `GaussiansDF` attributes.

gmm3.weights
gmm3.gaussiansDF.head(5)

# (4) Plot the Gaussian mixture model using the `plot_clusters` function.

plot_clusters(gmm3)

# (5) Apply the Gaussian mixture model to the `assembled` DataFrame using the `transform` method:

predictions = gmm3.transform(assembled)
predictions.printSchema()
predictions.select("features", "prediction", "probability").show(10, truncate=False)

# (6) Print the distribution of `gender` by cluster.

predictions.crosstab("prediction", "gender").orderBy("prediction_gender").show()

# (7) **Bonus:** Plot the distribution of `gender` by cluster.

pdf = predictions.select("gender", "prediction").fillna("missing", subset=["gender"]).toPandas()
sns.countplot(data=pdf, x="prediction", hue="gender", hue_order=["female", "male", "missing"])


# ## References

# [Wikipedia - Cluster analysis](https://en.wikipedia.org/wiki/Cluster_analysis)

# [Spark Documentation - Clustering](http://spark.apache.org/docs/latest/ml-clustering.html)

# [Spark Python API - pyspark.ml.clustering.GaussianMixture class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixture)

# [Spark Python API - pyspark.ml.clustering.GaussianMixtureModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixtureModel)

# [Spark Python API - pyspark.ml.clustering.GaussianMixtureSummary class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixtureSummary)


# ## Stop the SparkSession

spark.stop()
# # Processing Text and Fitting and Evaluating Topic Models

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * Topic modeling algorithms such as Latent Dirichlet allocation extract
# themes from a set of text documents

# * Clustering algorithms such as K-means and Gaussian mixture models assume
# that an observation belongs to one and only one cluster

# * Latent Dirichlet allocation assumes that each document belongs to one or
# more topics

# * The number of topics is a hyperparameter

# * Topic models can be used to categorize news articles


# ## Scenario

# In this demonstration we will use latent Dirichlet allocation (LDA) to look
# for topics in the ride reviews.  We will also perform some basic natural
# language processing (NLP) to prepare the data for LDA.


# ## Setup

# None


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the (clean) ride review data from HDFS:
reviews = spark.read.parquet(S3_ROOT + "/duocar/clean/ride_reviews/")
reviews.head(5)


# ## Extract and transform features

# The ride reviews are not in a form amenable to machine learning algorithms.
# Spark MLlib provides a number of feature extractors and feature transformers
# to preprocess the ride reviews into a form appropriate for modeling.


# ### Parse the ride reviews

# Use the
# [Tokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Tokenizer)
# class to tokenize the reviews:
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="review", outputCol="words")
tokenized = tokenizer.transform(reviews)
tokenized.drop("ride_id").head(5)

# Note that punctuation is not being handled properly.  Use the
# [RegexTokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RegexTokenizer)
# class to improve the tokenization:
from pyspark.ml.feature import RegexTokenizer
tokenizer = RegexTokenizer(inputCol="review", outputCol="words", gaps=False, pattern="[a-zA-Z-']+")
tokenized = tokenizer.transform(reviews)
tokenized.drop("ride_id").head(5)

# The arguments `gaps` and `pattern` are set to extract words consisting of
# lowercase letters, uppercase letters, hyphens, and apostrophes.

# Define a function to plot a word cloud:
def plot_word_cloud(df, col):
  # Compute the word count:
  from pyspark.sql.functions import explode
  word_count = df.select(explode(col).alias("word")).groupBy("word").count().collect()
  # Generate the word cloud image:
  from wordcloud import WordCloud
  wordcloud = WordCloud(random_state=12345).fit_words(dict(word_count))
  # Display the word cloud image:
  import matplotlib.pyplot as plt
  plt.imshow(wordcloud, interpolation="bilinear")
  plt.axis("off")

# Plot the word cloud:
plot_word_cloud(tokenized, "words")


# ### Remove common (stop) words from each review

# Note that the ride reviews contain a number of common words such as "the"
# that we do not expect to be relevant.
# Use the
# [StopWordsRemover](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StopWordsRemover)
# class to remove these so-called *stop words*:
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="words", outputCol="relevant_words")
remover.getStopWords()[:10]
removed = remover.transform(tokenized)
removed.select("words", "relevant_words").head(5)

# Plot the word cloud:
plot_word_cloud(removed, "relevant_words")


# ### Count the frequency of words in each review

# Use the
# [CountVectorizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.CountVectorizer)
# class to compute the term frequency:
from pyspark.ml.feature import CountVectorizer
vectorizer = CountVectorizer(inputCol="relevant_words", outputCol="word_count_vector", vocabSize=100)

# The `fit` method computes the top $N$ words where $N$ is set via the
# `vocabSize` hyperparameter:
vectorizer_model = vectorizer.fit(removed)
vectorizer_model.vocabulary[:10]

# The `transform` method counts the number of times each vocabulary word
# appears in each review:
vectorized = vectorizer_model.transform(removed)
vectorized.select("words", "word_count_vector").head(5)

# **Note:** The resulting word count vector is stored as a
# [SparseVector](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.linalg.SparseVector).


# ## Specify and fit a topic model using latent Dirichlet allocation (LDA)

# Use the `LDA` class to specify an LDA model:
from pyspark.ml.clustering import LDA
lda = LDA(featuresCol="word_count_vector", k=2, seed=23456)

# Use the `explainParams` method to examine additional hyperparameters:
print(lda.explainParams())

# Use the `fit` method to fit the LDA model:
lda_model = lda.fit(vectorized)

# The resulting model is an instance of the `LDAModel` class:
type(lda_model)


# ## Examine the LDA topic model fit

lda_model.logLikelihood(vectorized)
lda_model.logPerplexity(vectorized)


# ## Examine the LDA topic model

# Examine the estimated distribution of topics:
lda_model.estimatedDocConcentration()

# Examine the estimated distribution of words for each topic:
lda_model.topicsMatrix()

# Examine the most important words in each topic:
lda_model.describeTopics().head(5)

# Plot the terms and weights for each topic:
def plot_topics(model, n_terms, vocabulary):
  import matplotlib.pyplot as plt
  import seaborn as sns
  rows = model.describeTopics(n_terms).collect()
  for row in rows:
    title = "Topic %s" % row["topic"]
    x = row["termWeights"]
    y = [vocabulary[i] for i in row["termIndices"]]
    plt.figure()
    sns.barplot(x, y)
    plt.title(title)
    plt.xlabel("Weight")
plot_topics(lda_model, 5, vectorizer_model.vocabulary)

 
# ## Apply the topic model

predictions = lda_model.transform(vectorized)
predictions.select("review", "topicDistribution").head(5)


# ## Exercises

# (1) Fit an LDA model with $k=3$ topics.

# * Use the `setK` method to change the number of topics for the `lda` instance:

# * Use the `fit` method to fit the LDA model to the `vectorized` DataFrame:

# * Use the `plot_topics` function to examine the topics:

# * Use the `transform` method to apply the LDA model to the `vectorized` DataFrame:

# * Print out a few rows of the transformed DataFrame:

# (2) Use the `NGram` transformer to generate pairs of words (bigrams) from the tokenized reviews.

# * Import the `NGram` class from the `pyspark.ml.feature` module:

# * Create an instance of the `NGram` class:

# * Use the `transform` method to apply the `NGram` instance to the `tokenized` DataFrame:

# * Print out a few rows of the transformed DataFrame:


# ## References

# [Wikipedia - Topic model](https://en.wikipedia.org/wiki/Topic_model)

# [Wikipedia - Latent Dirichlet
# allocation](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)

# [Spark Documentation - Latent Dirichlet
# allocation](http://spark.apache.org/docs/latest/ml-clustering.html#latent-dirichlet-allocation-lda)

# [Spark Python API - pyspark.ml.clustering.LDA
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDA)

# [Spark Python API - pyspark.ml.clustering.LDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDAModel)

# [Spark Python API - pyspark.ml.clustering.LocalLDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LocalLDAModel)

# [Spark Python API - psypark.ml.clustering.DistributedLDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.DistributedLDAModel)


# ## Stop the SparkSession

spark.stop()
# # Processing Text and Fitting and Evaluating Topic Models - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * Topic modeling algorithms such as Latent Dirichlet allocation extract
# themes from a set of text documents

# * Clustering algorithms such as K-means and Gaussian mixture models assume
# that an observation belongs to one and only one cluster

# * Latent Dirichlet allocation assumes that each document belongs to one or
# more topics

# * The number of topics is a hyperparameter

# * Topic models can be used to categorize news articles


# ## Scenario

# In this demonstration we will use latent Dirichlet allocation (LDA) to look
# for topics in the ride reviews.  We will also perform some basic natural
# language processing (NLP) to prepare the data for LDA.


# ## Setup

# None


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the (clean) ride review data from HDFS:
reviews = spark.read.parquet(S3_ROOT + "/duocar/clean/ride_reviews/")
reviews.head(5)


# ## Extract and transform features

# The ride reviews are not in a form amenable to machine learning algorithms.
# Spark MLlib provides a number of feature extractors and feature transformers
# to preprocess the ride reviews into a form appropriate for modeling.


# ### Parse the ride reviews

# Use the
# [Tokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Tokenizer)
# class to tokenize the reviews:
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="review", outputCol="words")
tokenized = tokenizer.transform(reviews)
tokenized.drop("ride_id").head(5)

# Note that punctuation is not being handled properly.  Use the
# [RegexTokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RegexTokenizer)
# class to improve the tokenization:
from pyspark.ml.feature import RegexTokenizer
tokenizer = RegexTokenizer(inputCol="review", outputCol="words", gaps=False, pattern="[a-zA-Z-']+")
tokenized = tokenizer.transform(reviews)
tokenized.drop("ride_id").head(5)

# The arguments `gaps` and `pattern` are set to extract words consisting of
# lowercase letters, uppercase letters, hyphens, and apostrophes.

# Define a function to plot a word cloud:
def plot_word_cloud(df, col):
  # Compute the word count:
  from pyspark.sql.functions import explode
  word_count = df.select(explode(col).alias("word")).groupBy("word").count().collect()
  # Generate the word cloud image:
  from wordcloud import WordCloud
  wordcloud = WordCloud(random_state=12345).fit_words(dict(word_count))
  # Display the word cloud image:
  import matplotlib.pyplot as plt
  plt.imshow(wordcloud, interpolation="bilinear")
  plt.axis("off")

# Plot the word cloud:
plot_word_cloud(tokenized, "words")


# ### Remove common (stop) words from each review

# Note that the ride reviews contain a number of common words such as "the"
# that we do not expect to be relevant.
# Use the
# [StopWordsRemover](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StopWordsRemover)
# class to remove these so-called *stop words*:
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="words", outputCol="relevant_words")
remover.getStopWords()[:10]
removed = remover.transform(tokenized)
removed.select("words", "relevant_words").head(5)

# Plot the word cloud:
plot_word_cloud(removed, "relevant_words")


# ### Count the frequency of words in each review

# Use the
# [CountVectorizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.CountVectorizer)
# class to compute the term frequency:
from pyspark.ml.feature import CountVectorizer
vectorizer = CountVectorizer(inputCol="relevant_words", outputCol="word_count_vector", vocabSize=100)

# The `fit` method computes the top $N$ words where $N$ is set via the
# `vocabSize` hyperparameter:
vectorizer_model = vectorizer.fit(removed)
vectorizer_model.vocabulary[:10]

# The `transform` method counts the number of times each vocabulary word
# appears in each review:
vectorized = vectorizer_model.transform(removed)
vectorized.select("words", "word_count_vector").head(5)

# **Note:** The resulting word count vector is stored as a
# [SparseVector](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.linalg.SparseVector).


# ## Specify and fit a topic model using latent Dirichlet allocation (LDA)

# Use the `LDA` class to specify an LDA model:
from pyspark.ml.clustering import LDA
lda = LDA(featuresCol="word_count_vector", k=2, seed=23456)

# Use the `explainParams` method to examine additional hyperparameters:
print(lda.explainParams())

# Use the `fit` method to fit the LDA model:
lda_model = lda.fit(vectorized)

# The resulting model is an instance of the `LDAModel` class:
type(lda_model)


# ## Examine the LDA topic model fit

lda_model.logLikelihood(vectorized)
lda_model.logPerplexity(vectorized)


# ## Examine the LDA topic model

# Examine the estimated distribution of topics:
lda_model.estimatedDocConcentration()

# Examine the estimated distribution of words for each topic:
lda_model.topicsMatrix()

# Examine the most important words in each topic:
lda_model.describeTopics().head(5)

# Plot the terms and weights for each topic:
def plot_topics(model, n_terms, vocabulary):
  import matplotlib.pyplot as plt
  import seaborn as sns
  rows = model.describeTopics(n_terms).collect()
  for row in rows:
    title = "Topic %s" % row["topic"]
    x = row["termWeights"]
    y = [vocabulary[i] for i in row["termIndices"]]
    plt.figure()
    sns.barplot(x, y)
    plt.title(title)
    plt.xlabel("Weight")
plot_topics(lda_model, 5, vectorizer_model.vocabulary)

 
# ## Apply the topic model

predictions = lda_model.transform(vectorized)
predictions.select("review", "topicDistribution").head(5)


# ## Exercises

# (1) Fit an LDA model with $k=3$ topics.

# * Use the `setK` method to change the number of topics for the `lda` instance:

lda.setK(3)

# * Use the `fit` method to fit the LDA model to the `vectorized` DataFrame:

lda_model = lda.fit(vectorized)

# * Use the `plot_topics` function to examine the topics:

plot_topics(lda_model, 5, vectorizer_model.vocabulary)

# * Use the `transform` method to apply the LDA model to the `vectorized` DataFrame:

predictions = lda_model.transform(vectorized)

# * Print out a few rows of the transformed DataFrame:

predictions.select("review", "topicDistribution").head(5) 

# (2) Use the `NGram` transformer to generate pairs of words (bigrams) from the tokenized reviews.

# * Import the `NGram` class from the `pyspark.ml.feature` module:

from pyspark.ml.feature import NGram

# * Create an instance of the `NGram` class:

ngramer = NGram(inputCol="words", outputCol="bigrams", n=2)

# * Use the `transform` method to apply the `NGram` instance to the `tokenized` DataFrame:

ngramed = ngramer.transform(tokenized)

# * Print out a few rows of the transformed DataFrame:

ngramed.printSchema()
ngramed.select("words", "bigrams").head(5)


# ## References

# [Wikipedia - Topic model](https://en.wikipedia.org/wiki/Topic_model)

# [Wikipedia - Latent Dirichlet
# allocation](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)

# [Spark Documentation - Latent Dirichlet
# allocation](http://spark.apache.org/docs/latest/ml-clustering.html#latent-dirichlet-allocation-lda)

# [Spark Python API - pyspark.ml.clustering.LDA
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDA)

# [Spark Python API - pyspark.ml.clustering.LDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDAModel)

# [Spark Python API - pyspark.ml.clustering.LocalLDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LocalLDAModel)

# [Spark Python API - psypark.ml.clustering.DistributedLDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.DistributedLDAModel)


# ## Stop the SparkSession

spark.stop()
# # Fitting and Evaluating Recommender Models

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Recommender Models

# * *Recommender models* help users find relevant items in a very large set
# (when they do not know what they are looking for):
#   * Products
#   * Books
#   * Music
#   * Movies

# * A recommender model is usually part of a larger *recommender system* that
# includes a user interface to present recommendations and gather user
# preferences.

# * There are two main classes of recommender algorithms:
#   * Content based
#   * Collaborative filtering

# * *Collaborative filtering* algorithms recommend items based on the
# preferences of "similar" users.
#   * Explicit preferences (e.g., star rating)
#   * Implicit preferences (e.g., artist playcount)


# * Collaborative filtering algorithms fall somewhere between supervised and
# unsupervised learning algorithms.
#   * Some item preferences are known
#   * Most item preferences are unknown

# * Spark MLlib provides the *alternating least squares* (ALS) algorithm for
# collaborative filtering

# * The ALS algorithm takes a DataFrame with user, item, and rating columns as input.


# ## Scenario

# Le DuoCar is owned by a private equity (PE) firm.  This PE firm has a
# streaming music company in its portfolio called Earcloud. Earcloud is having
# a hard time hiring data scientists in this competitive environment, so the PE
# firm has asked the DuoCar data science team to help the Earcloud analysts and
# engineers develop a musical artist recommender system.  Earcloud has provided
# Apache web server logs from which we can extract the user's listening
# behavior.  We will use this behavior to fit and evaluate a collaborative
# filtering model.


# ## Setup

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract


# ## Create a SparkSession

import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )


# ## Read the data

# Read the Apache access logs from Earcloud:
access_logs = spark.read.text(S3_ROOT + "/duocar/earcloud/apache_logs/")
access_logs.printSchema()
access_logs.head(5)
access_logs.count()


# ## Prepare the data for modeling

# Create user, artist, and playcount columns:
pattern = "^.*artist=(.*)&playcount=(.*) HTTP.*USER=(.*)\".*"
playcounts = access_logs.filter(col("value").contains("/listen?")) \
  .withColumn("user", regexp_extract("value", pattern, 3).cast("integer")) \
  .withColumn("artist", regexp_extract("value", pattern, 1).cast("integer")) \
  .withColumn("playcount", regexp_extract("value", pattern, 2).cast("integer")) \
  .persist()
playcounts.head(20)

# Note that the playcount column includes some negative, binary values:
playcounts.filter("playcount < 0").show()

# Fix the playcount column:
from pyspark.sql.functions import when, abs, conv
playcounts_fixed = playcounts.withColumn("playcount_fixed", when(col("playcount") < 0, conv(abs(col("playcount")), 2, 10).cast("integer")).otherwise(col("playcount")))
playcounts_fixed.printSchema()                            
playcounts_fixed.filter("playcount < 0").select("playcount", "playcount_fixed").show()

# **Note:** The `conv()` function returns a string.

# Select the modeling data:
recommendation_data = playcounts_fixed \
    .select("user", "artist", "playcount_fixed") \
    .withColumnRenamed("playcount_fixed", "playcount")
recommendation_data.show()

# Save the modeling data:
recommendation_data.write.parquet(S3_HOME + "/data/recommendation_data/", mode="overwrite")


# ## Create train and test datasets

(train, test) = recommendation_data.randomSplit(weights=[0.75, 0.25], seed=12345)


# ## Specify and fit an ALS model

from pyspark.ml.recommendation import ALS
als = ALS(userCol="user", itemCol="artist", ratingCol="playcount", implicitPrefs=True, seed=23456)
print(als.explainParams())
als.setColdStartStrategy("drop")
als_model = als.fit(train)


# ## Examine the ALS model

als_model.userFactors.head(5)
als_model.itemFactors.head(5)

# **Note:** Some artists are not represented in the training data:
als_model.userFactors.count()
als_model.itemFactors.count()


# ## Apply the model

train_predictions = als_model.transform(train)
train_predictions.printSchema()
train_predictions.sort("user").show()


# ## Evaluate the model

# Evaluate the model on the train dataset:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="playcount", metricName="rmse")
evaluator.evaluate(train_predictions)

# Evaluate the model on the test dataset:
test_predictions = als_model.transform(test)
evaluator.evaluate(test_predictions)


# ## Generate recommendations

# Recommend the top $n$ items for each user:
als_model.recommendForAllUsers(5).sort("user").head(5)

# Recommend the top $n$ users for each item:
als_model.recommendForAllItems(5).sort("artist").head(5)

# Generate recommendations for a subset of users or items by using the
# following methods:
#```python
#als_model.recommendForUserSubset
#als_model.recommendForItemSubset
#```


# ## Exercises

# None


# ## References

# [Spark Python API - pyspark.ml.recommendation.ALS
# class](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALS)

# [Spark Python API - pyspark.ml.recommendation.ALSModel
# class](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALSModel)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
# # Working with Machine Learning Pipelines

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# In the previous modules we established a workflow in which we loaded some
# data; preprocessed the data; extracted, transformed, and selected features;
# and fit and evaluated a machine learning model.  In this module we show how
# we can encapsulate this workflow into a [Spark MLlib
# Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml)
# that we can reuse in our development process or production environment.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )


# ## Load the data

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined_all/")


# ## Create the train and test sets

# Create the train and test sets *before* specifying the pipeline:
(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Specify the pipeline stages

# A *Pipeline* is a sequence of stages that implement a data engineering or
# machine learning workflow.  Each stage in the pipeline is either a
# *Transformer* or an *Estimator*.  Recall that a
# [Transformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Transformer)
# takes a DataFrame as input and returns a DataFrame as output.  Recall that an
# [Estimator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Estimator)
# takes a DataFrame as input and returns a Transformer (e.g., model) as output.
# We begin by specifying the stages in our machine learning workflow.

# Filter out the cancelled rides:
from pyspark.ml.feature import SQLTransformer
filterer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0")

# Generate the reviewed feature:
extractor = SQLTransformer(statement="SELECT *, review IS NOT NULL AS reviewed FROM __THIS__")

# Index `vehicle_color`:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_indexed")

# Encode `vehicle_color_indexed`:
from pyspark.ml.feature import OneHotEncoderEstimator
encoder = OneHotEncoderEstimator(inputCols=["vehicle_color_indexed"], outputCols=["vehicle_color_encoded"])

# Select and assemble the features:
from pyspark.ml.feature import VectorAssembler
features = ["reviewed", "vehicle_year", "vehicle_color_encoded", "CloudCover"]
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Specify the estimator (i.e., classification algorithm):
from pyspark.ml.classification import RandomForestClassifier
classifier = RandomForestClassifier(featuresCol="features", labelCol="star_rating", seed=23451)
print(classifier.explainParams())

# Specify the hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
paramGrid = ParamGridBuilder() \
  .addGrid(classifier.maxDepth, [5, 10, 20]) \
  .addGrid(classifier.numTrees, [20, 50, 100]) \
  .addGrid(classifier.subsamplingRate, [0.5, 1.0]) \
  .build()

# Specify the evaluator:
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="star_rating", metricName="accuracy")

# **Note:** We are treating `star_rating` as a multiclass label.

# Specify the validator:
from pyspark.ml.tuning import TrainValidationSplit
validator = TrainValidationSplit(estimator=classifier, estimatorParamMaps=paramGrid, evaluator=evaluator, seed=34512)


# ## Specify the pipeline

# A
# [Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Pipeline)
# itself is an `Estimator`:
from pyspark.ml import Pipeline
stages = [filterer, extractor, indexer, encoder, assembler, validator]
pipeline = Pipeline(stages=stages)


# ## Fit the pipeline model

# The `fit` method produces a
# [PipelineModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.PipelineModel),
# which is a `Transformer`:
%time pipeline_model = pipeline.fit(train)


# ## Inspect the pipeline model

# Access the stages of a `PipelineModel` instance using the `stages` attribute:
pipeline_model.stages

# ### Inspect the string indexer

indexer_model = pipeline_model.stages[2]
type(indexer_model)
indexer_model.labels

# ### Inspect the validator model

validator_model = pipeline_model.stages[5]
type(validator_model)
validator_model.validationMetrics

# ### Inspect the best random forest classifier

best_model = validator_model.bestModel
type(best_model)

# Inspect the best hyperparameters:
validator_model.bestModel._java_obj.getMaxDepth()
validator_model.bestModel.getNumTrees
validator_model.bestModel._java_obj.getSubsamplingRate()

# **Note:** We have to access the values for `maxDepth` and `subsamplingRate`
# from the underlying Java object.

# Plot the feature importances:
def plot_feature_importances(fi):
  fi_array = fi.toArray()
  plt.figure()
  sns.barplot(list(range(len(fi_array))), fi_array)
  plt.title("Feature Importances")
  plt.xlabel("Feature")
  plt.ylabel("Importance")
plot_feature_importances(validator_model.bestModel.featureImportances)


# ## Save and load the pipeline model

# Save the pipeline model object to our local directory in HDFS:
pipeline_model.write().overwrite().save(S3_HOME + "/models/pipeline_model")

# **Note**: We can use Hue to explore the saved object.

# We can also use the following convenience method if we do not need to
# overwrite an existing model:
#```python
#pipeline_model.save(S3_HOME + "/models/pipeline_model")
#```

# Load the pipeline model object from our local directory in HDFS:
from pyspark.ml import PipelineModel
pipeline_model_loaded = PipelineModel.read().load(S3_HOME + "/models/pipeline_model")

# We can also use the following convenience method:
#```python
#pipeline_model_loaded = PipelineModel.load(S3_HOME + "/models/pipeline_model")
#```

# Save the underlying Java object to get around an issue with saving
# `TrainValidationSplitModel()` objects:
pipeline_model._to_java().write().overwrite().save(S3_HOME + "/models/pipeline_model")


# ## Apply the pipeline model

# Use the `transform` method to apply the `PipelineModel` to the test set:
classified = pipeline_model_loaded.transform(test)
classified.printSchema()


# ## Evaluate the pipeline model

# Generate a confusion matrix:
classified \
  .groupBy("prediction") \
  .pivot("star_rating") \
  .count() \
  .orderBy("prediction") \
  .fillna(0) \
  .show()

# Evaluate the random forest model:
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="star_rating", metricName="accuracy")
evaluator.evaluate(classified)

# Compare to the baseline prediction (always predict five-star rating):
from pyspark.sql.functions import lit
classified_with_baseline = classified.withColumn("prediction_baseline", lit(5.0))
evaluator.setPredictionCol("prediction_baseline").evaluate(classified_with_baseline)

# Our random forest classifier is doing no better than always predicting a
# five-star rating.  We can try to improve our model by adding more features,
# experimenting with additional hyperparameter combinations, and exploring
# other machine learning algorithms.


# ## Exercises

# (1) Import the
# [RFormula](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RFormula)
# class from the `pyspark.ml.feature` module.

# (2) Create an instance of the `RFormula` class with the R formula
# `star_rating ~ reviewed + vehicle_year + vehicle_color`.

# (3) Specify a pipeline consisting of the `filterer`, `extractor`, and the
# RFormula instance specified above.

# (4) Fit the pipeline on the `train` DataFrame.

# (5) Use the `save` method to save the pipeline model to the
# `models/my_pipeline_model` directory in HDFS.

# (6) Import the `PipelineModel` class from the `pyspark.ml` package.

# (7) Use the `load` method of the `PipelineModel` class to load the saved
# pipeline model.

# (8) Apply the loaded pipeline model to the test set and examine the resulting
# DataFrame.


# ## References

# [Spark Documentation - ML Pipelines](http://spark.apache.org/docs/latest/ml-pipeline.html)

# [Spark Python API - pyspark.ml package](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html)

# [Spark Python API - MLReader class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.util.MLReader)

# [Spark Python API - MLWriter class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.util.MLWriter)


# ## Stop the SparkSession

spark.stop()
# # Working with Machine Learning Pipelines - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# In the previous modules we established a workflow in which we loaded some
# data; preprocessed the data; extracted, transformed, and selected features;
# and fit and evaluated a machine learning model.  In this module we show how
# we can encapsulate this workflow into a [Spark MLlib
# Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml)
# that we can reuse in our development process or production environment.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )


# ## Load the data

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined_all/")


# ## Create the train and test sets

# Create the train and test sets *before* specifying the pipeline:
(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Specify the pipeline stages

# A *Pipeline* is a sequence of stages that implement a data engineering or
# machine learning workflow.  Each stage in the pipeline is either a
# *Transformer* or an *Estimator*.  Recall that a
# [Transformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Transformer)
# takes a DataFrame as input and returns a DataFrame as output.  Recall that an
# [Estimator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Estimator)
# takes a DataFrame as input and returns a Transformer (e.g., model) as output.
# We begin by specifying the stages in our machine learning workflow.

# Filter out the cancelled rides:
from pyspark.ml.feature import SQLTransformer
filterer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0")

# Generate the reviewed feature:
extractor = SQLTransformer(statement="SELECT *, review IS NOT NULL AS reviewed FROM __THIS__")

# Index `vehicle_color`:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_indexed")

# Encode `vehicle_color_indexed`:
from pyspark.ml.feature import OneHotEncoderEstimator
encoder = OneHotEncoderEstimator(inputCols=["vehicle_color_indexed"], outputCols=["vehicle_color_encoded"])

# Select and assemble the features:
from pyspark.ml.feature import VectorAssembler
features = ["reviewed", "vehicle_year", "vehicle_color_encoded", "CloudCover"]
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Specify the estimator (i.e., classification algorithm):
from pyspark.ml.classification import RandomForestClassifier
classifier = RandomForestClassifier(featuresCol="features", labelCol="star_rating", seed=23451)
print(classifier.explainParams())

# Specify the hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
paramGrid = ParamGridBuilder() \
  .addGrid(classifier.maxDepth, [5, 10, 20]) \
  .addGrid(classifier.numTrees, [20, 50, 100]) \
  .addGrid(classifier.subsamplingRate, [0.5, 1.0]) \
  .build()

# Specify the evaluator:
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="star_rating", metricName="accuracy")

# **Note:** We are treating `star_rating` as a multiclass label.

# Specify the validator:
from pyspark.ml.tuning import TrainValidationSplit
validator = TrainValidationSplit(estimator=classifier, estimatorParamMaps=paramGrid, evaluator=evaluator, seed=34512)


# ## Specify the pipeline

# A
# [Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Pipeline)
# itself is an `Estimator`:
from pyspark.ml import Pipeline
stages = [filterer, extractor, indexer, encoder, assembler, validator]
pipeline = Pipeline(stages=stages)


# ## Fit the pipeline model

# The `fit` method produces a
# [PipelineModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.PipelineModel),
# which is a `Transformer`:
%time pipeline_model = pipeline.fit(train)


# ## Inspect the pipeline model

# Access the stages of a `PipelineModel` instance using the `stages` attribute:
pipeline_model.stages

# ### Inspect the string indexer

indexer_model = pipeline_model.stages[2]
type(indexer_model)
indexer_model.labels

# ### Inspect the validator model

validator_model = pipeline_model.stages[5]
type(validator_model)
validator_model.validationMetrics

# ### Inspect the best random forest classifier

best_model = validator_model.bestModel
type(best_model)

# Inspect the best hyperparameters:
validator_model.bestModel._java_obj.getMaxDepth()
validator_model.bestModel.getNumTrees
validator_model.bestModel._java_obj.getSubsamplingRate()

# **Note:** We have to access the values for `maxDepth` and `subsamplingRate`
# from the underlying Java object.

# Plot the feature importances:
def plot_feature_importances(fi):
  fi_array = fi.toArray()
  plt.figure()
  sns.barplot(list(range(len(fi_array))), fi_array)
  plt.title("Feature Importances")
  plt.xlabel("Feature")
  plt.ylabel("Importance")
plot_feature_importances(validator_model.bestModel.featureImportances)


# ## Save and load the pipeline model

# Save the pipeline model object to our local directory in HDFS:
pipeline_model.write().overwrite().save(S3_HOME + "/models/pipeline_model")

# **Note**: We can use Hue to explore the saved object.

# We can also use the following convenience method if we do not need to
# overwrite an existing model:
#```python
#pipeline_model.save(S3_HOME + "/models/pipeline_model")
#```

# Load the pipeline model object from our local directory in HDFS:
from pyspark.ml import PipelineModel
pipeline_model_loaded = PipelineModel.read().load(S3_HOME + "/models/pipeline_model")

# We can also use the following convenience method:
#```python
#pipeline_model_loaded = PipelineModel.load(S3_HOME + "/models/pipeline_model")
#```

# Save the underlying Java object to get around an issue with saving
# `TrainValidationSplitModel()` objects:
pipeline_model._to_java().write().overwrite().save(S3_HOME + "/models/pipeline_model")


# ## Apply the pipeline model

# Use the `transform` method to apply the `PipelineModel` to the test set:
classified = pipeline_model_loaded.transform(test)
classified.printSchema()


# ## Evaluate the pipeline model

# Generate a confusion matrix:
classified \
  .groupBy("prediction") \
  .pivot("star_rating") \
  .count() \
  .orderBy("prediction") \
  .fillna(0) \
  .show()

# Evaluate the random forest model:
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="star_rating", metricName="accuracy")
evaluator.evaluate(classified)

# Compare to the baseline prediction (always predict five-star rating):
from pyspark.sql.functions import lit
classified_with_baseline = classified.withColumn("prediction_baseline", lit(5.0))
evaluator.setPredictionCol("prediction_baseline").evaluate(classified_with_baseline)

# Our random forest classifier is doing no better than always predicting a
# five-star rating.  We can try to improve our model by adding more features,
# experimenting with additional hyperparameter combinations, and exploring
# other machine learning algorithms.


# ## Exercises

# (1) Import the
# [RFormula](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RFormula)
# class from the `pyspark.ml.feature` module.

from pyspark.ml.feature import RFormula

# (2) Create an instance of the `RFormula` class with the R formula
# `star_rating ~ reviewed + vehicle_year + vehicle_color`.

rformula = RFormula(formula = "star_rating ~ reviewed + vehicle_year + vehicle_color")

# (3) Specify a pipeline consisting of the `filterer`, `extractor`, and the
# RFormula instance specified above.

pipeline = Pipeline(stages=[filterer, extractor, rformula])

# (4) Fit the pipeline on the `train` DataFrame.

pipeline_model = pipeline.fit(train)

# (5) Use the `save` method to save the pipeline model to the
# `models/my_pipeline_model` directory in HDFS.

pipeline_model.write().overwrite().save(S3_HOME + "/models/my_pipeline_model")

# (6) Import the `PipelineModel` class from the `pyspark.ml` package.

from pyspark.ml import PipelineModel

# (7) Use the `load` method of the `PipelineModel` class to load the saved
# pipeline model.

pipeline_model_loaded = PipelineModel.load(S3_HOME + "/models/my_pipeline_model")
                                           
# (8) Apply the loaded pipeline model to the test set and examine the resulting
# DataFrame.

test_transformed = pipeline_model_loaded.transform(test)
test_transformed.printSchema()
test_transformed.select("features", "label").show(truncate=False)


# ## References

# [Spark Documentation - ML Pipelines](http://spark.apache.org/docs/latest/ml-pipeline.html)

# [Spark Python API - pyspark.ml package](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html)

# [Spark Python API - MLReader class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.util.MLReader)

# [Spark Python API - MLWriter class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.util.MLWriter)


# ## Stop the SparkSession

spark.stop()
# # Applying a scikit-learn Model to a Spark DataFrame

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# We might want to use a machine learning algorithm that is not supported by
# the Spark MLlib library.  In this case we can use our favorite Python package
# to develop our machine learning model on a sample of data (on a single
# machine) and then use a Spark UDF to apply the model to the full data (in our
# Hadoop environment).  In this module we use the scikit-learn package to
# rebuild the isotonic regression model we built earlier using Spark MLlib.  We
# then use a pandas UDF to apply the model to a Spark DataFrame.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ## Prepare the data using Spark

# We want to prepare our data and generate our features using Spark.
# Otherwise, we will have to replicate any changes that we make to our sample
# data on our full data before we can apply our machine learning model in
# Spark.

# ### Start a SparkSession

import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# ### Load the data

rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
rides.printSchema()

# ### Prepare the regression data

# Select the feature and the label and filter out the cancelled rides:
regression_data = rides.select("distance", "duration").where("cancelled = FALSE")


# ## Build a scikit-learn model

# We will use the [scikit-learn](https://scikit-learn.org/stable/) package to
# develop an [isotonic
# regression](https://en.wikipedia.org/wiki/Isotonic_regression) model.

# ### Select a sample of data and load it into a pandas DataFrame

modeling_data = regression_data \
  .sample(withReplacement=False, fraction=0.1, seed=12345) \
  .toPandas()

# ### Plot the data

modeling_data.plot(x="distance", y="duration", kind="scatter", color="orange")

# ### Specify the features and label

features = modeling_data['distance']
label = modeling_data['duration']

# ### Create train and test sets

from sklearn.model_selection import train_test_split
features_train, features_test, label_train, label_test = train_test_split(features, label, test_size=0.3, random_state=42)

# ### Specify and fit an isotonic regression model

from sklearn.isotonic import IsotonicRegression
ir = IsotonicRegression(out_of_bounds="clip")
ir.fit(features_train, label_train)

# **Note:** The `out_of_bounds` argument determines how feature values outside
# the range of training values are handled during prediction.

# ### Evaluate the isotonic regression model

from math import sqrt
from sklearn.metrics import mean_squared_error

# Apply the model to the train and test datasets:
label_train_predicted = ir.predict(features_train)
label_test_predicted = ir.predict(features_test)

# Compute the RMSE on the train and test datasets:
sqrt(mean_squared_error(label_train, label_train_predicted))
sqrt(mean_squared_error(label_test, label_test_predicted))

# **Note:** These RMSE values are consistent with our results using Spark
# MLlib.

# ### Plot the isotonic regression model

def plot_model():
  fig = plt.figure()
  plt.scatter(features_test, label_test, s=12, c="orange", alpha=0.25)
  # Use plt.plot rather than plt.step since sklearn does linear interpolation.
  plt.plot(ir.f_.x, ir.f_.y, color="black")
  plt.title("Isotonic Regression on Test Set")
  plt.xlabel("Distance (m)")
  plt.ylabel("Duration (s)")
plot_model()

# ### Save the model for later use in CML Native Workbench

# Use the pickle package to serialize the model on disk:
import pickle
with open("ir_model.pickle", "wb") as f:
  pickle.dump(ir, f)


# ## Apply the model using a Spark UDF

# We will use the more efficient pandas UDF, which processes multiple rows of
# data rather than a single row of data, to apply our model to a Spark
# DataFrame.  The Spark executors must have access to our model.  We can let
# Spark automatically *broadcast* the model to the executors or we can manually
# broadcast it.

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType

# ### Automatically broadcast the model to the executors

# Define and register our pandas UDF:
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def predict_udf(x):
  return pd.Series(ir.predict(x))

# Normally our UDF will take multiple columns representing multiple features
# (or a single column representing a feature vector) and it will reshape the
# input into a form appropriate for the specific model predict method.

# The Spark driver will distribute the *closure* of the Python function to the
# Spark executors.  The closure consists of the function and its environment.
# In this case the closure includes the `ir` instance and its `predict` method.

# Use the UDF to apply the model to our Spark DataFrame:
regression_data.withColumn("duration_predicted", predict_udf("distance")).show(5)

# **Note:** You will have to deal with null (missing) values before calling the
# UDF or handle them within the UDF.

# **Important:** This code will fail when running Spark via YARN if `pyarrow`
# is not installed on all the worker nodes.

# ### Manually broadcast the model to the executors

# Rather than let the Spark driver distribute our model, we can explicitly
# broadcast it to the Spark executors:
bc_ir_model = spark.sparkContext.broadcast(ir)

# Define and register our pandas UDF:
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def bc_predict_udf(x):
  return pd.Series(bc_ir_model.value.predict(x))

# Use the UDF to apply the model to our Spark DataFrame:
data_with_prediction = regression_data.withColumn("duration_predicted", bc_predict_udf("distance"))
data_with_prediction.show(5)

# ### Evaluate the model

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="duration_predicted", labelCol="duration", metricName="rmse")
evaluator.evaluate(data_with_prediction)


# ## Exercises

# None


# ## References

# [scikit-learn Documentation - IsotonicRegression
# class](https://scikit-learn.org/stable/modules/generated/sklearn.isotonic.IsotonicRegression.html?highlight=isotonic#sklearn-isotonic-isotonicregression)

# [Spark Python API - pandas_udf
# function](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

# [Spark Programming Guide - Broadcast
# Variables](https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#broadcast-variables)


# ## Cleanup

spark.stop()
# # Deploying a Machine Learning Model as a Rest API in CML Native Workbench

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# CML allows us to easily deploy our model
# and serve predictions via a REST API.  The first step in the process is to
# create a [wrapper function](https://en.wikipedia.org/wiki/Wrapper_function)
# around our model predict function that receives features in JSON format,
# applies the model, and returns the prediction in JSON format.  In this module
# we demonstrate how we can wrap our scikit-learn isotonic regression model for
# deployment in CML.  The [CML
# documentation](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-model-training-deployment.html)
# provides further details on actually deploying the model and obtaining
# predictions.

# **Important:** You must run `22_deploy_udf.py` before running this script.


# ## Setup

import pickle
import numpy as np


# ## Load the serialized model

with open("ir_model.pickle", "rb") as f:
  ir_model = pickle.load(f)


# ## Define a wrapper function to generate a prediction

def predict_cdsw(json_input):
  
  # Extract the features from the JSON input
  # (which looks like a Python dict object):
  distance = json_input["distance"]
  
  if distance is not None:
    
    # Reshape the features for the predict method:
    features = np.array([distance])
  
    # Compute the prediction:
    prediction = ir_model.predict(features)
  
    # Assemble the prediction into JSON object:
    json_output = {"duration": prediction[0]}
    
  else:
    json_output = {"duration": None}
  
  return json_output


# ## Test the function

predict_cdsw({"distance": 10000})
predict_cdsw({"distance": None})


# ## References

# [CML Documentation - Models](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-model-training-deployment.html)
