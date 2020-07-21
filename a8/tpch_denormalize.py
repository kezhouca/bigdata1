from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession,functions

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(keyspace,out_keyspace):
    orders=spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace).load()
    orders.createOrReplaceTempView('orders')
    lineitem=spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace).load()
    lineitem.createOrReplaceTempView('lineitem')
    part=spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace).load()
    part.createOrReplaceTempView('part')
    df=spark.sql('select o.orderkey, p.name from orders o join lineitem l on o.orderkey=l.orderkey join part p on l.partkey=p.partkey order by o.orderkey')
    df=df.groupBy(df.orderkey).agg(functions.collect_set(df.name).alias('part_names'))
    df.createOrReplaceTempView('part_names')
    orders_parts=spark.sql('select o.*, p.part_names from orders o join part_names p on o.orderkey=p.orderkey')
    orders_parts.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=out_keyspace).save()

if __name__ == '__main__':
    keyspace = sys.argv[1]
    out_keyspace= sys.argv[2]
    main(keyspace,out_keyspace)

