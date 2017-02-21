import time
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext

if __name__=="__main__":

	# Create Spark Context
	sc = SparkContext("local[2]","Application")
	sqlContext = SQLContext(sc)
	filepath = "C:\\Users\\6910P\\Google Drive\\Dalhousie\\term_1\\data_management_analytics\\assignment_3\\NationalNames.csv"
	
	# Load data.
	df = sqlContext.read.load(filepath,format='com.databricks.spark.csv', header='true', inferSchema='true')
	df.registerTempTable("babynames")
	
	start_time = time.time()
	# Total number of birth registered in a year
	birth_count_by_year = sqlContext.sql("SELECT year, SUM(count) FROM babynames GROUP BY year ORDER BY year ASC")
	print "Processing Time:"+str((time.time() - start_time)*1000)+" msec.\nTotal number of birth registered in a year:"
	birth_count_by_year.show(birth_count_by_year.count(),False)
	