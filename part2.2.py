from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col

spark = SparkSession.builder.appName("ServerLogAnalysis").getOrCreate()
log_file = "2024_07_11.request.log" 
logs = spark.read.text(log_file)

# regexp for logfile
log_pattern = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'

#parsing
parsed_logs = logs.select(regexp_extract('value', log_pattern, 1).alias('ip'),
                          regexp_extract('value', log_pattern, 4).alias('date'),
                          regexp_extract('value', log_pattern, 5).alias('method'),
                          regexp_extract('value', log_pattern, 6).alias('url'),
                          regexp_extract('value', log_pattern, 8).cast('integer').alias('status'),
                          regexp_extract('value', log_pattern, 9).cast('integer').alias('content_size'))

# filter 200
successful_requests = parsed_logs.filter(col('status') == 200)
ip_counts = successful_requests.groupBy('ip').count().orderBy('count', ascending=False)
status_counts = parsed_logs.groupBy('status').count().orderBy('status')

# print results
print("Top 10 IP addresses by request count:")
ip_counts.show(10, truncate=False)

print("\nRequest counts by status:")
status_counts.show(truncate=False)

spark.stop()