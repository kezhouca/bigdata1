from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def load_jason(line):
    return json.loads(line) 

def filter_for_subreddit(c):
	return 'e' in c['subreddit']

def filter_for_positive(c):
	return c['score']>0

def filter_for_negative(c):
	return c['score']<=0

def main(inputs, output):
	rdd= sc.textFile(inputs).map(load_jason).filter(filter_for_subreddit).cache()
	rdd.filter(filter_for_positive).map(json.dumps).saveAsTextFile(output + '/positive')
	rdd.filter(filter_for_negative).map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

