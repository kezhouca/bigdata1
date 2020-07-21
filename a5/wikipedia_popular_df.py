import sys,re,string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

page_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.IntegerType()),
])

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
	parts=path.split('/')
	return parts[len(parts)-1][11:22]

def main(inputs, output):
	pages=spark.read.csv(inputs,page_schema,' ').withColumn('filename', functions.input_file_name())
	pages=pages.withColumn('hour',path_to_hour(pages.filename))
	pages=pages.where(pages.language=='en').where(pages.title!='Main_Page').where(~pages.title.startswith('Special')).cache()
	maxviews = pages.groupby(pages.hour).max('views').orderBy(pages.hour).withColumnRenamed('hour','hour1')
	maxviews = maxviews.join(pages,(pages.hour==maxviews.hour1) & (pages.views==maxviews['max(views)'])).select(pages.hour,pages.title,pages.views).orderBy(pages.hour)
	maxviews.write.json(output,mode='overwrite')
	maxviews.explain()

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

