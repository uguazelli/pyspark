import operator
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import floor,months_between,when,udf,max,substring,lit,col,regexp_replace
from pyspark.sql.types import IntegerType,FloatType,DateType


def odd_even(id):
	return 0 if (id % 2 == 0) else 1
		
def main():

	sc = SparkContext()
	sqlContext = SQLContext(sc)
	
	# Load Parquet
	#test = sqlContext.load('file:///home/cloudera/Documents/pyspark/MOCK_DATA.json', header=True)
	
	# Load JSON
	df = sqlContext.read.json('file:///home/cloudera/Documents/pyspark/MOCK_DATA.json')	
	df.show()
	df.printSchema()
	
	# Transform function to spark
	is_odd_red = udf(odd_even, IntegerType())
	 
	# Add new column whith python function result
	df_result = df.withColumn("id_odd", is_odd_red( df["id"] ) )	
	df_result.show()
	
	df_result_simplify = df_result.select('id','id_odd')
	df_result_simplify.show()
	
if __name__ == "__main__":
    main()
