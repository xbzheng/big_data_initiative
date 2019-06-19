#Author: Ivan Zheng
#Last update: 6/17/2019

from pyspark import SparkContext

def word_count(input_file_path, output_file_path):
    sc = SparkContext()
    lines = sc.textFile(input_file_path)
    words = lines.flatMap(lambda line : line.split(" "))
    tuples = words.map(lambda word : (word, 1))
    counts = tuples.reduceByKey(lambda a, b: (a + b))
    counts.coalesce(1).saveAsTextFile(output_file_path)

if __name__ == "__main__":
    input_file_path = "../data/words.txt"
    output_file_path = "out.txt"
    word_count(input_file_path, output_file_path)

