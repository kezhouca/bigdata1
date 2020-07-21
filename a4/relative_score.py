from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def load_json(line):
    return json.loads(line) 
    

def add_pairs(x, y):
	count1,score1=x
	count2,score2=y
	return (count1+count2,score1+score2)

def average(kv):
	k,v=kv
	count,score=v
	return (k,score/count)

def main(inputs, output):
	commentdata = sc.textFile(inputs).map(load_json).cache()
	avgdata = commentdata.map(lambda c:(c['subreddit'],(1,c['score']))).reduceByKey(add_pairs).map(average)
	commentbysub = commentdata.map(lambda c: (c['subreddit'], c)) 
	avgdata.join(commentbysub).map(lambda x: (x[1][1]['author'], x[1][1]['score']/x[1][0])).sortBy(lambda x:x[1],ascending=False).map(json.dumps).saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
