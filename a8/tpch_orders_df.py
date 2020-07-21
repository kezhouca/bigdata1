from pyspark import SparkConf, SparkContext
import sys,re,uuid,math
from datetime import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession,functions

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def output_format(x):
    orderkey,price,names =x
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def main(keyspace,outdir,orderkeys):
    orders=spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace).load()
    orders.createOrReplaceTempView('orders')
    lineitem=spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace).load()
    lineitem.createOrReplaceTempView('lineitem')
    part=spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace).load()
    part.createOrReplaceTempView('part')
    df=spark.sql('select o.*,p.name from orders o join lineitem l on o.orderkey=l.orderkey join part p on l.partkey=p.partkey where o.orderkey in ('+', '.join(orderkeys)+') order by o.orderkey')
    df=df.groupBy(df.orderkey,df.totalprice).agg(functions.collect_set(df.name).alias('names'))
    df.explain()
    df.rdd.map(output_format).saveAsTextFile(outdir)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir= sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,outdir,orderkeys)
