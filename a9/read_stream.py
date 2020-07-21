import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession,functions
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))    
    values_xy = split(values.value, ' ')
    values = values.withColumn('x', values_xy.getItem(0))
    values = values.withColumn('y', values_xy.getItem(1))
    values = values.withColumn('xy', values.x * values.y)
    values = values.withColumn('xx', values.x ** 2)
    s = values.agg(count('x').alias('n'),sum('x').alias('x'),sum('y').alias('y'),sum('xy').alias('xy'),sum('xx').alias('xx'))
    s = s.withColumn('b',(s.xy-s.x*s.y/s.n)/(s.xx-s.x**2/s.n))
    s = s.withColumn('a',(s.y-s.b*s.x)/s.n) 
    stream = s.select('a','b').writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(600)
    stream.stop()

if __name__ == '__main__':
    topic=sys.argv[1]
    main(topic)

