import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'  # Use localhost

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, count, regexp_extract
import matplotlib.pyplot as plt
import requests

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

spark = SparkSession.builder \
    .appName("LargeArticleAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

article_url = "https://gist.githubusercontent.com/dupuy/1855764/raw/338c3d9f4aab955aab32b673707f1aa2d09d7e7f/README.rst" 
local_filename = "part2.3.txt"

download_file(article_url, local_filename)

df = spark.read.text(local_filename)

words_df = df.select(
    explode(
        split(lower(col("value")), "\\W+")
    ).alias("word")
).where(
    (col("word") != "") & 
    (~col("word").rlike("\\d"))  #remove numeric data
)

word_counts = words_df.groupBy("word").count().orderBy("count", ascending=False)

top_20_words = word_counts.limit(20).collect()

#graph (gistogram)_
plt.figure(figsize=(12, 6))
plt.bar([row['word'] for row in top_20_words], [row['count'] for row in top_20_words])
plt.title('Top 20 Words in the Article')
plt.xlabel('Words')
plt.ylabel('Frequency')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('word_histogram.png')
plt.close()

#output
print("Top 20 words:")
for row in top_20_words:
    print(f"{row['word']}: {row['count']}")

most_common_word = top_20_words[0]['word']
print(f"\nThe most common word in the article is: '{most_common_word}'")

total_words = words_df.count()
print(f"\nTotal number of words in the article: {total_words}")


spark.stop()