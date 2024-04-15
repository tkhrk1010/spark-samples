from pyspark.sql import SparkSession
import sys

# SparkSessionの作成
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Set log level to WARN and redirect logs to stderr
spark.sparkContext.setLogLevel("INFO")
# log4jとは、Javaのログ出力ライブラリ
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.INFO)
log4j.LogManager.getRootLogger().addAppender(log4j.ConsoleAppender(log4j.PatternLayout("%p %t %m%n"), "System.out"))


# テキストファイルの読み込み
text_file = spark.read.text("input.txt")

# 一行目をスペース区切りで単語に分割し、単語ごとに1つのペアを作成
# RDD(Resilient Distributed Dataset)は、Sparkの基本的なデータ構造
# flatMap は、各要素を複数の要素に展開する関数
words = text_file.rdd.flatMap(lambda line: line[0].split(" "))

# 各単語の出現回数を数える
# map は、各要素に関数を適用する関数
# ここでは、各単語をキー、1(個)を値とするペアに変換している
# reduceByKey は、キーごとに値を集約する関数
# 同じwordごとに、1(個)を足し合わせている
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# 結果の表示
print("Word counts:")
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# SparkSessionの停止
spark.stop()