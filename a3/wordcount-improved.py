from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import re, string
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

def words_once(line):
    result = wordsep.split(line);
    for w in result:
        yield (w,1)

def add(x, y):
    return x + y

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
	sc.textFile(inputs).repartition(80).flatMap(words_once).filter(lambda x:x[0]!="").reduceByKey(add).sortByKey().map(output_format).saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
