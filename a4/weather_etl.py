import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary

def main(inputs, output):
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

	weather = spark.read.csv(inputs, schema=observation_schema)
	etl_data=weather.select(weather['station'],weather['date'],(weather['value']/10).alias('tmax')).where(weather['qflag'].isNull()).where(weather['observation']=='TMAX').where(functions.substring(weather['station'],0,2)=='CA')
	etl_data.write.json(output, compression='gzip', mode='overwrite')
	etl_data.show()
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
