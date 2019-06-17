#Author: Ivan Zheng
#Last update: 6/17/2019

import pyspark
lines = sc.textFile("hdfs:/user/cloudera/words.txt")
lines.count()
words = lines.flatMap(lambda line : line.split(" "))
tuples = words.map(lambda word : (word, 1))
counts = tuples.reduceByKey(lambda a, b: (a + b))
#save to the HDFS
counts.coalesce(1).saveAsTextFile('hdfs:/user/cloudera/wordcount/outputDir')
