from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, count, sum as _sum


spark = SparkSession.builder.appName("WordCountCSV").getOrCreate()

input_file = "API_UKR_DS2_en_csv_v2_3412264.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)

text_column = 'Indicator Name'

if text_column not in df.columns:
    print(f"Помилка: Колонка '{text_column}' не знайдена в CSV файлі.")
    print("Доступні колонки:")
    print(df.columns)
    spark.stop()
    exit(1)


word_counts = df.select(explode(split(lower(col(text_column)), "\\W+")).alias("word")) \
    .filter(col("word") != "") \
    .groupBy("word") \
    .agg(count("*").alias("count")) \
    .orderBy("count", ascending=False)


print("Top words:")
total_words = word_counts.agg(_sum("count")).collect()[0][0]

print("All words and their counts:")
word_counts.show(word_counts.count(), truncate=False)

print(f"\nTotal number of words: {total_words}")

# Збереження результатів у текстовий файл
output_file = "word_counts_result.txt"
word_counts_collected = word_counts.collect()

with open(output_file, "w", encoding="utf-8") as f:
    for row in word_counts_collected:
        f.write(f"{row['word']}: {row['count']}\n")
    f.write(f"\nTotal number of words: {total_words}\n")

print(f"Results saved to {output_file}")

# Зупинка SparkSession
spark.stop()