from pyspark import SparkConf, SparkContext
import sys,re,uuid
from datetime import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

line_re = re.compile(r'^(\S+) - - \[(\S+ [+-]\d+)\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
schema = types.StructType([
	#types.StructField('id', types.StringType()),
    types.StructField('host', types.StringType()),
    types.StructField('datetime', types.TimestampType()),
    types.StructField('path', types.StringType()),
    types.StructField('bytes', types.IntegerType())])

def read_line(line):
	m=line_re.match(line)
	if m is None:
		return None
	return (m.group(1),datetime.strptime(m.group(2),'%d/%b/%Y:%H:%M:%S %z'),m.group(3),int(m.group(4)))

def main(input_dir,keyspace,table):
	text = sc.textFile(input_dir).repartition(10)
	rdd = text.map(read_line).filter(lambda x:x is not None)
	uuid_udf=functions.udf(lambda : str(uuid.uuid4()),types.StringType())
	df= spark.createDataFrame(rdd,schema).withColumn('id',uuid_udf())
	df.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).save()

if __name__ == '__main__':
	input_dir = sys.argv[1]
	keyspace = sys.argv[2]
	table = sys.argv[3]
	main(input_dir,keyspace,table)

