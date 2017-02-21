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

# Function to get the month from date string of format 02/12/2016	
def get_month(date):
	return int(date[:2])
	
# Function to get the quarter from date string of format 02/12/2016	
def get_quarter(date):
	month = int(date[:2])
	if(month <4):	
		return "1"
	elif (month > 3) and (month <7):
		return "2"
	elif (month > 6) and (month <10):
		return "3"
	else:
		return "4"
	
	
if __name__=="__main__":

	#sc = SparkContext("local[2]","Application")
	data_file  = get_data_file(True)
	sqlContext = SQLContext(sc)
	
	# Load the data
	df = sqlContext.read.load(data_file,format='com.databricks.spark.csv', header='true', inferSchema='true')
	df.registerTempTable("nypdmvcollisions")
	
	print "Count of total records:"+str(df.count())
	
	udfDateToYear=udf(get_year, StringType())
	df_with_year = df.withColumn("year", udfDateToYear("DATE"))
	df_with_year.registerTempTable("nypdmvcollisions_year")
	
	udfDateToQuarter=udf(get_quarter, StringType())
	df_with_year_quart = df_with_year.withColumn("quarter", udfDateToQuarter("DATE"))
	df_with_year_quart.registerTempTable("nypdmvcollisions_year_quart")
	
	start_time = time.time()
	#Capture total injuries and incident count(sum of injuries and fatalities) grouped by Borough, year and month
	udfDateToQuarterMonth=udf(get_month, StringType())
	df_with_year_quart_month = df_with_year.withColumn("month", udfDateToQuarterMonth("DATE"))
	df_with_year_quart_month.registerTempTable("nypdmvcollisions_year_quart_month")
	query_4 = """SELECT BOROUGH , year , month, 
	(SUM(NUMBER_OF_PERSONS_KILLED) + SUM(NUMBER_OF_PEDESTRIANS_KILLED) +SUM(NUMBER_OF_CYCLIST_KILLED)+ SUM(NUMBER_OF_MOTORIST_KILLED) +
	SUM(NUMBER_OF_PERSONS_INJURED)+SUM(NUMBER_OF_PEDESTRIANS_INJURED)+SUM(NUMBER_OF_CYCLIST_INJURED)+ SUM(NUMBER_OF_MOTORIST_INJURED)) 
	AS All_Injuries_Incidents_Count	from nypdmvcollisions_year_quart_month GROUP BY BOROUGH, year, month ORDER BY 
	year DESC, month ASC"""
	query_4_sql = sqlContext.sql(query_4)
	print "Processing Time:"+str((time.time() - start_time)*1000)+" msec.\ntotal injuries and incident count grouped by Borough, year and month"
	query_4_sql.show(query_4_sql.count(),False)