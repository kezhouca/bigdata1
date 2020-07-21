import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3
import datetime
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from pyspark.ml import PipelineModel
tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(out_model):
    d1=datetime.date(2019, 11, 8)
    d2=datetime.date(2019, 11, 9)
    df=spark.createDataFrame([('RSM00031235', d1,49.2771,-122.9146,330.0,12.0),\
        ('RSM00031235', d2,49.2771,-122.9146,330.0,14.0)],\
        tmax_schema)
    df.show()

    model = PipelineModel.load(out_model)
    predictions = model.transform(df)
    predictions.show()
    
    result=predictions.select("prediction").collect()
    prediction=result[0]['prediction']
    print('Predicted tmax tomorrow:',prediction)

if __name__ == '__main__':
    out_model=sys.argv[1]
    main(out_model)

