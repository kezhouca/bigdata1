import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import *

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(inputs,out_model):
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    query ='SELECT today.*, dayofyear(today.date) as doy, yesterday.tmax AS yesterday_tmax \
    FROM __THIS__ as today \
    INNER JOIN __THIS__ as yesterday \
    ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station'
    sql_transformer =  SQLTransformer(statement=query)
    #feature_assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'doy'], outputCol='features')
    feature_assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'doy', 'yesterday_tmax'], outputCol='features')
    #estimator=GBTRegressor(featuresCol='features',labelCol='tmax',predictionCol='prediction')
    estimator=DecisionTreeRegressor(featuresCol='features',labelCol='tmax',predictionCol='prediction')
    pipeline =  Pipeline(stages=[sql_transformer, feature_assembler,estimator])
    model=pipeline.fit(train)   
    predictions = model.transform(validation)
    predictions.show()

    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print('r2=',r2)
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    print('rmse=',rmse)
    model.write().overwrite().save(out_model)

if __name__ == '__main__':
    inputs = sys.argv[1]
    out_model=sys.argv[2]
    main(inputs,out_model)
