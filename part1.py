import psutil
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("PySpark Lab").setMaster("local")
sc = SparkContext(conf=conf) #sc init

spark = SparkSession.builder.appName("PySpark Lab").getOrCreate()

data = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "banana", "apple"]
rdd = sc.parallelize(data)

upper_rdd = rdd.map(lambda x: x.upper())
print("Слова великими літерами:", upper_rdd.collect())

b_words = rdd.filter(lambda x: x.startswith('b'))
print("Слова, що починаються з 'B':", b_words.collect())

distinct_rdd = rdd.distinct()
print("Унікальні слова:", distinct_rdd.collect())

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
num_rdd = sc.parallelize(numbers)

squared_rdd = num_rdd.map(lambda x: x**2)
print("Числа в квадраті:", squared_rdd.collect())

even_rdd = num_rdd.filter(lambda x: x % 2 == 0)
print("Парні числа:", even_rdd.collect())

even_count = even_rdd.count()
print("Кількість парних чисел:", even_count)

sum_all = num_rdd.sum()
print("Сума всіх чисел:", sum_all)

product_rdd = sc.parallelize(numbers)
product_all = product_rdd.reduce(lambda x, y: x * y)
print("Добуток всіх чисел:", product_all)


print("Перший підрахунок:", cached_rdd.count())
print("Другий підрахунок:", cached_rdd.count())
print("Третій підрахунок:", cached_rdd.count())

sc.stop()