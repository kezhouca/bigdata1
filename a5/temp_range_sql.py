import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
    ])

def main(inputs, output):
	weather = spark.read.csv(inputs, schema=observation_schema)
	weather.createOrReplaceTempView('weather')
	tmax_data=spark.sql("select date,station,value/10 as tmax from weather where qflag is null and observation='TMAX'")
	tmax_data.createOrReplaceTempView('tmax_data')
	tmin_data=spark.sql("select date,station,value/10 as tmin from weather where qflag is null and observation='TMIN'")
	tmin_data.createOrReplaceTempView('tmin_data')
	range_data=spark.sql('select tmax_data.date,tmax_data.station,tmax_data.tmax-tmin_data.tmin as range from tmax_data join tmin_data on tmax_data.date=tmin_data.date and tmax_data.station=tmin_data.station order by tmax_data.date,tmax_data.station')
	range_data.createOrReplaceTempView('range_data')
	max_range_data=spark.sql('select date,max(range) as range from range_data group by date order by date')
	max_range_data.createOrReplaceTempView('max_range_data')
	result=spark.sql('select range_data.date,range_data.station,round(range_data.range,1) from max_range_data join range_data on max_range_data.date=range_data.date and max_range_data.range=range_data.range order by range_data.date')
	result.write.csv(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
