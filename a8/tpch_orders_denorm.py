from pyspark import SparkConf, SparkContext
import sys
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
    orders_parts=spark.read.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=keyspace).load()
    orders_parts.createOrReplaceTempView('orders_parts')
    df=spark.sql('select orderkey,totalprice,part_names from orders_parts where orderkey in ('+','.join(orderkeys)+')')
    df.rdd.map(output_format).saveAsTextFile(outdir)
    
if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir= sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,outdir,orderkeys)

