from pyspark import SparkConf, SparkContext
import sys,re,math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
schema = types.StructType([
    types.StructField('hostname', types.StringType()),
    types.StructField('bytes', types.IntegerType())])

def read_line(line):
	m=line_re.match(line)
	if m is None:
		return None
	return (m.group(1),int(m.group(4)))

def main(inputs):
	text = sc.textFile(inputs)
	rdd = text.map(read_line).filter(lambda x:x is not None)
	logs= spark.createDataFrame(rdd,schema)
	logs.createOrReplaceTempView('logs')
	stat=spark.sql('select count(hostname) as x, sum(bytes) as y from logs group by hostname')
	stat.createOrReplaceTempView('stat')
	result=spark.sql('select count(x) as n, sum(x) as sumx, sum(y) as sumy, sum(x*x) as sumx2,sum(y*y) as sumy2, sum(x*y) as sumxy from stat')
	row=result.collect()[0]
	n=row.n
	sumx=row.sumx
	sumy=row.sumy
	sumx2=row.sumx2
	sumy2=row.sumy2
	sumxy=row.sumxy
	r=(n*sumxy-sumx*sumy)/(math.sqrt(n*sumx2-sumx*sumx)*math.sqrt(n*sumy2-sumy*sumy))
	print('r=',r)
	print('r^2=',r*r)

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)

