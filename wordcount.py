from pyspark import SparkContext

sc = SparkContext("local", "WordCount")

# Read the input file
text_file = sc.textFile("sample.txt")

# Split into words
words = text_file.flatMap(lambda line: line.split(" "))

# Map each word to a (word, 1) pair
word_pairs = words.map(lambda word: (word, 1))

# Reduce by key (word) to count occurrences
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Collect and print the results
for word, count in word_counts.collect():
    print(f"{word}: {count}")

sc.stop()

