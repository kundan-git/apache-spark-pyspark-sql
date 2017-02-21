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
	# Input a year and populate top 5 most popular names registered that year
	top_5_names_in_year = sqlContext.sql("SELECT DISTINCT name, count FROM babynames WHERE year=1880 ORDER BY count DESC")
	print  "Processing Time:"+str((time.time() - start_time)*1000)+" msec.\nInput a year and populate top 5 most popular names registered that year"
	top_5_names_in_year.show(top_5_names_in_year.count(),False)
	