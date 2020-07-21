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
	weather = spark.read.csv(inputs, schema=observation_schema).cache()
	tmax_data=weather.select(weather.station,weather.date,(weather.value/10).alias('tmax')).where(weather.qflag.isNull()).where(weather.observation=='TMAX')
	tmin_data=weather.select(weather.station,weather.date,(weather.value/10).alias('tmin')).where(weather.qflag.isNull()).where(weather.observation=='TMIN')
	range_data=tmax_data.join(tmin_data,(tmin_data.date==tmax_data.date) & (tmin_data.station==tmax_data.station)).withColumn('range',tmax_data.tmax-tmin_data.tmin).select(tmax_data.date,tmax_data.station,'range').orderBy(tmax_data.date)
	max_range_data=range_data.groupBy(range_data.date).agg(functions.max(range_data.range).alias('range1')).withColumnRenamed('date','date1')
	result=max_range_data.join(range_data,(max_range_data.date1==range_data.date) & (max_range_data.range1==range_data.range)).select(range_data.date,range_data.station,functions.round(range_data.range,1)).orderBy(range_data.date)
	result.write.csv(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
