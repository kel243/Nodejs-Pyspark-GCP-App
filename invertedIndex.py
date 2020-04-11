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

output = rdd.flatMap(lambda (path, contents):[(path, word) for word in re.sub(r'[^a-zA-Z0-9]', ' ', contents).lower().split()])\
    .map(lambda (path, word): ((word,path), 1))\
    .reduceByKey(lambda a, b: a + b)\
    .map(lambda ((word, path), count): (word, dirName(path), fileName(path), count))
	
if mode == 'top-n':
	n = int(sys.argv[3])
	topOutput = output.map(lambda el: (el[0], el[3]))\
		.reduceByKey(lambda a, b: a + b)\
		.sortBy(lambda el: -el[1]).take(n)
	topOutput = sc.parallelize(topOutput)
	topOutput.saveAsTextFile('gs://dataproc-staging-us-west1-22809130792-qxcrlltp/outputs/topOutput')
elif mode == 'term':
	term = sys.argv[3]
	termOutput = output.filter(lambda el: el[0] == term).take(20)
	termOutput = sc.parallelize(termOutput)
	termOutput.saveAsTextFile('gs://dataproc-staging-us-west1-22809130792-qxcrlltp/outputs/termOutput')
elif mode == 'index':
	indexOutput = output.take(20)
	indexOutput = sc.parallelize(indexOutput)
	indexOutput.saveAsTextFile('gs://dataproc-staging-us-west1-22809130792-qxcrlltp/outputs/indexOutput')






