from pyspark import SparkConf, SparkContext
import sys,math
from datetime import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(keyspace,table):
    df=spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    df.createOrReplaceTempView('logs')
    stat=spark.sql('select count(host) as x, sum(bytes) as y from logs group by host')
    stat.createOrReplaceTempView('stat')
    result=spark.sql('select count(x) as n, sum(x) as sumx, sum(y) as sumy, sum(x*x) as sumx2,sum(y*y) as sumy2, sum(x*y) as sumxy from stat')
    row=result.collect()[0]
    n=row.n
    sumx=row.sumx
    sumy=row.sumy
    sumx2=row.sumx2
    sumy2=row.sumy2
    sumxy=row.sumxy
    r=(n*sumxy-sumx*sumy)/(math.sqrt(n*sumx2-sumx**2)*math.sqrt(n*sumy2-sumy**2))
    print('r=',r)
    print('r^2=',r**2)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]
    main(keyspace,table)

