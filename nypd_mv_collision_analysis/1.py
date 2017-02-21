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
		
if __name__=="__main__":

	sc = SparkContext("local[2]","Application")
	data_file  = get_data_file(True)
	sqlContext = SQLContext(sc)
	
	# Load the data
	df = sqlContext.read.load(data_file,format='com.databricks.spark.csv', header='true', inferSchema='true')
	df.registerTempTable("nypdmvcollisions")
	
	print "Count of total records:"+str(df.count())
	
	start_time = time.time()
	
	# Capture total injuries and fatalities associated with each motor collision record(identified by a unique incident key)
	query =  """SELECT UNIQUE_KEY, (  NUMBER_OF_PERSONS_KILLED  + NUMBER_OF_PEDESTRIANS_KILLED  + NUMBER_OF_CYCLIST_KILLED + 
		NUMBER_OF_MOTORIST_KILLED) AS All_Fatalities_Count, 
		(NUMBER_OF_PERSONS_INJURED+NUMBER_OF_PEDESTRIANS_INJURED+NUMBER_OF_CYCLIST_INJURED+NUMBER_OF_MOTORIST_INJURED) As All_Injured_Count 
		FROM nypdmvcollisions """
	record_by_key = sqlContext.sql(query)
	print "Processing Time:"+str((time.time() - start_time)*1000)+" msec.\nTotal injuries and fatalities associated with each motor collision record:"
	record_by_key.show(1000,False)