import time
import urllib
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Get the file path, if not local download from web.
def get_data_file(isLocal):
	if(isLocal):
		filepath = "C:\\Users\\6910P\\Google Drive\\Dalhousie\\term_1\\data_management_analytics\\assignment_3\\NYPD_Motor_Vehicle_Collisions\\NYPD_Motor_Vehicle_Collisions.csv"
		return filepath
	else:
		resource_url ="https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv"
		urllib.urlretrieve("https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv","NYPD_Motor_Vehicle_Collisions.csv")
		return "NYPD_Motor_Vehicle_Collisions.csv"
		
# Function to get the year from date string of format 02/12/2016
def get_year(date):
	return date[-4:]
	
if __name__=="__main__":

	sc = SparkContext("local[2]","Application")
	data_file  = get_data_file(True)
	sqlContext = SQLContext(sc)
	
	# Load the data
	df = sqlContext.read.load(data_file,format='com.databricks.spark.csv', header='true', inferSchema='true')
	df.registerTempTable("nypdmvcollisions")
	
	start_time = time.time()
	#Capture total incident counts in a year (grouped by year)
	udfDateToYear=udf(get_year, StringType())
	df_with_year = df.withColumn("year", udfDateToYear("DATE"))
	df_with_year.registerTempTable("nypdmvcollisions_year")
	query_2 = "SELECT year, COUNT(*) as cnt from nypdmvcollisions_year GROUP BY year ORDER BY cnt DESC"
	query_2_sql = sqlContext.sql(query_2)
	print "Processing Time:"+str((time.time() - start_time)*1000)+" msec."
	query_2_sql.show(query_2_sql.count(),False)