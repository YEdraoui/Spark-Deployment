from pyspark import SparkContext
import re

# Regular expression pattern for Nginx logs (Common Log Format)
log_pattern = re.compile(
    r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d{3}) (\S+) "(.*?)" "(.*?)"'
)

def parse_log_line(logline):
    match = log_pattern.match(logline)
    if match:
        # Extract the request field (e.g., GET /index.html HTTP/1.1)
        request = match.group(5)
        # Split the request to get the URL
        parts = request.split()
        if len(parts) >= 2:
            url = parts[1]
            return url
    return None

if __name__ == "__main__":
    sc = SparkContext(appName="NginxLogAnalysis")
    # Read log files from HDFS
    logs = sc.textFile("hdfs:///user/your_username/logs/nginx_log*.log")
    # Parse logs and extract URLs
    urls = logs.map(parse_log_line).filter(lambda x: x is not None)
    # Count visits per page
    url_counts = urls.map(lambda url: (url, 1)).reduceByKey(lambda a, b: a + b)
    # Sort pages by number of visits in descending order
    sorted_url_counts = url_counts.sortBy(lambda x: x[1], ascending=False)
    # Collect and print results
    results = sorted_url_counts.collect()
    for url, count in results:
        print(f"{url} {count}")
    sc.stop()

