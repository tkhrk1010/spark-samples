from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WordCountDataFrame") \
    .getOrCreate()

# Set log level to INFO and redirect logs to stdout
spark.sparkContext.setLogLevel("WARN")
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
log4j.LogManager.getRootLogger().addAppender(log4j.ConsoleAppender(log4j.PatternLayout("%p %t %m%n"), "System.out"))

# Read the CSV file into a DataFrame
# データフレームは、スキーマを持つ行と列のデータ構造で、RDDよりも高度な抽象化を提供する。
# csvを読み込むときに、行ごとに自動的にパーティションに分割される。
# パーティションのサイズは、Sparkの設定（spark.sql.files.maxPartitionBytes）や、ファイルのサイズと場所に基づいて決定される。
df = spark.read.csv("input.csv", header=True, inferSchema=True)

# Show the schema of the DataFrame
print("DataFrame schema:")
df.printSchema()

# Show the first 5 rows of the DataFrame
print("First 5 rows of the DataFrame:")
df.show(5)

# Count the number of rows in the DataFrame
print(f"Number of rows in the DataFrame: {df.count()}")

# Select specific columns and perform operations
result_df = df.select(col("word"), col("count").cast("int")) \
    .groupBy("word") \
    .sum("count") \
    .orderBy(desc("sum(count)"))

# Show the result DataFrame
print("Result DataFrame:")
result_df.show()

# Stop the SparkSession
spark.stop()