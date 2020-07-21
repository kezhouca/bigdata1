from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def break_up(line):
    parts = line.split(" ")
    result =(parts[0],parts[1],parts[2],int(parts[3]),int(parts[4]))
    yield result

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

text=sc.textFile(inputs)
tuples = text.flatMap(break_up)
data =tuples.filter(lambda x:x[1]=="en" and x[2]!="Main Page" and not x[2].startswith("Special:"))
rdd=data.map(lambda x:(x[0],(x[3],x[2])))
max_count = rdd.reduceByKey(max).sortByKey() # (lambda x:x[0])
max_count.map(tab_separated).saveAsTextFile(output)
