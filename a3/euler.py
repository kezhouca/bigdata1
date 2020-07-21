from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import random

def f(x):
	iterations = 0
	random.seed()
	for y in range(x):
		sum = 0
		while sum < 1:
			sum += random.uniform(0,1)
			iterations=iterations+1
	return iterations

def g(a,b):
	return a+b

def main(inputs, output):
	samples = int(inputs)
	numSlices= 1000000
	total_iterations = sc.parallelize([int(samples/numSlices)]*numSlices,numSlices).map(f).reduce(g)
	print(total_iterations/samples)
	
if __name__ == '__main__':
    conf = SparkConf().setAppName('euler by ke')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = "" #sys.argv[2]
    main(inputs, output)
