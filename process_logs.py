from pyspark import SparkContext
import re

# Initialize SparkContext
sc = SparkContext(appName="NginxLogProcessingHDFS")

# Read log files from HDFS
log_files = sc.textFile("hdfs://localhost:9000/nginx/input/*")

# Function to extract the page URL from a log line
def extract_page(line):
    match = re.search(r'GET\s+(\S+)\s+HTTP', line)
    if match:
        return match.group(1)
    else:
        return None

# Process the logs
visits = (log_files
          .map(extract_page)
          .filter(lambda x: x is not None)
          .map(lambda page: (page, 1))
          .reduceByKey(lambda a, b: a + b)
          .sortBy(lambda x: -x[1]))

# Collect and print the results
for page, count in visits.collect():
    print(f"{page} {count}")

# Optionally, save the results to HDFS
# visits.saveAsTextFile("hdfs:///nginx/output/")

# Stop the SparkContext
sc.stop()

