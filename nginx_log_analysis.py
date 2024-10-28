from pyspark import SparkContext
import re

sc = SparkContext("local", "NginxLogAnalysis")

# Define a regular expression pattern to extract the page path
log_pattern = r'GET\s+(\S+)\s+HTTP'

# Read the log files
logs = sc.textFile("access_log1,access_log2")

# Extract page paths
pages = logs.map(lambda line: re.search(log_pattern, line)) \
            .filter(lambda match: match is not None) \
            .map(lambda match: match.group(1))

# Map each page to a (page, 1) pair
page_pairs = pages.map(lambda page: (page, 1))

# Reduce by key to count page visits
page_counts = page_pairs.reduceByKey(lambda a, b: a + b)

# Sort by count in descending order
sorted_page_counts = page_counts.sortBy(lambda pair: pair[1], ascending=False)

# Collect and print the results
for page, count in sorted_page_counts.collect():
    print(f"{page} {count}")

sc.stop()

