from pyspark import SparkContext

sc = SparkContext("local", "PageCount")

# Load Nginx log data
log_data = sc.textFile("/mnt/data/nginx_log1.txt") \
           .union(sc.textFile("/mnt/data/nginx_log2.txt"))

# Filter out lines that don't have at least 7 elements
filtered_log_data = log_data.filter(lambda line: len(line.split(" ")) > 6)

# Extract page names (the request path is the 6th element) and count visits
page_visits = filtered_log_data.map(lambda line: line.split(" ")[6]) \
                               .map(lambda page: (page, 1)) \
                               .reduceByKey(lambda a, b: a + b) \
                               .sortBy(lambda x: -x[1])

# Save the result to the output directory
page_visits.saveAsTextFile("/mnt/data/output/page_visits")

sc.stop()

