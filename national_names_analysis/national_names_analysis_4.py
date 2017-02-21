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
	# Input a child name and populate total number of birth registrations throughout the dataset for that name
	birth_count_by_Name = sqlContext.sql("SELECT SUM(Count) FROM babynames WHERE Name='Mary'")
	print  "Processing Time:"+str((time.time() - start_time)*1000)+" msec.\nInput a child name and populate total number of birth registrations throughout the dataset for that name:"
	birth_count_by_Name.show(birth_count_by_Name.count(),False)
	