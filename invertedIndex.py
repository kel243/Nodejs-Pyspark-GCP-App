from pyspark import SparkContext    
import re
import sys
sc = SparkContext("local", "app")

filePath = sys.argv[1]
mode = sys.argv[2]

path = 'gs://dataproc-staging-us-west1-22809130792-qxcrlltp/' + filePath
rdd = sc.wholeTextFiles(path)

# Get file name from path
def fileName(path):
	arr = path.split('/')
	return arr[-1]

# Get file directory from path
def dirName(path):
	arr = path.split('/')
	return arr[-2]

# Format the files and map the content into (word, folder, file, count)
output = rdd.flatMap(lambda (path, contents):[(path, word) for word in re.sub(r'[^a-zA-Z0-9]', ' ', contents).lower().split()])\
    .map(lambda (path, word): ((word,path), 1))\
    .reduceByKey(lambda a, b: a + b)\
    .map(lambda ((word, path), count): (word, dirName(path), fileName(path), count))

# top-n search
if mode == 'top-n':
	n = int(sys.argv[3])
	# format into tuple of (word, count) and sum up all the tuples with the same word
	# then, take only n amount
	topOutput = output.map(lambda el: (el[0], el[3]))\
		.reduceByKey(lambda a, b: a + b)\
		.sortBy(lambda el: -el[1]).take(n)
	topOutput = sc.parallelize(topOutput)
	topOutput.saveAsTextFile('gs://dataproc-staging-us-west1-22809130792-qxcrlltp/outputs/topOutput')
elif mode == 'term': # term search
	term = sys.argv[3]
	# filter output by word == term and take 20
	termOutput = output.filter(lambda el: el[0] == term).take(20)
	termOutput = sc.parallelize(termOutput)
	termOutput.saveAsTextFile('gs://dataproc-staging-us-west1-22809130792-qxcrlltp/outputs/termOutput')
elif mode == 'index':
	# take 20 elements from the output to print out
	indexOutput = output.take(20)
	indexOutput = sc.parallelize(indexOutput)
	indexOutput.saveAsTextFile('gs://dataproc-staging-us-west1-22809130792-qxcrlltp/outputs/indexOutput')






