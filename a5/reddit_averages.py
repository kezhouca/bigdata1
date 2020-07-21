from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def map(line):
    x = json.loads(line) 
    yield (x["subreddit"],(1,x["score"]))

def add_pairs(x, y):
    return (x[0]+y[0],x[1]+ y[1])

def output_format(kv):
	k,v=kv
	return json.dumps([k,v[1]/v[0]])

def main(inputs, output):
	text = sc.textFile(inputs)
	outdata = text.flatMap(map).reduceByKey(add_pairs).sortByKey().map(output_format)
	outdata.saveAsTextFile(output)
                             

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
