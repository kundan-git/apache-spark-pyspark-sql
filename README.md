# apache-spark-pyspark-sql
Data analysis using Apache Spark, pyspark-sql and Pandas.

Demonstrates following data-analysis queries:
<table>
<tr> 
	<td></td>
	<td>Data Set</td>
	<td>Queries</td>
	<td>Application Path</td>
</tr>
<tr>
	<td>1.</td>
	<td>Dataset for word count</td>
	<td>Count distinct words and number of occurrences of each word in the dataset. </td> 
	<td>word_count/word_count.py </td> 
</tr>
    
<tr>
	<td rowspan=4>2.</td>
	<td rowspan=4> National Names dataset</td>
	<td >Total number of birth registered in a year</td>
	<td>national_names_analysis/1.py</td>
</tr>
<tr>
	<td >Total number of births registered in a year by gender</td>
	<td>national_names_analysis/2.py</td>
</tr>
<tr>
	<td >Input a year and populate top 5 most popular names registered that year </td>
	<td>national_names_analysis/3.py</td>
</tr>

<tr>
	<td >Input a child name and populate total number of birth registrations throughout the dataset for that name</td>
	<td>national_names_analysis/4.py</td>
</tr>


<tr>
	<td rowspan=4>3.</td>
	<td rowspan=4>NYPD Motor Vehicles Collision dataset</td>
	<td >Capture total injuries and fatalities associated with each motor collision record(identified by a unique incident key) </td>
	<td>nypd_mv_collision_analysis/1.py</td>
</tr>

<tr>
	<td >Capture total incident counts in a year (grouped by year)</td>
	<td>nypd_mv_collision_analysis/2.py</td>
</tr>

<tr>
	<td >Capture total injuries(can be sum of injuries and fatalities) grouped by year and quarter </td>
	<td>nypd_mv_collision_analysis/3.py</td>
</tr>

<tr>
	<td >Capture total injuries(sum of injuries and fatalities) and incident count grouped by Borough, year and month </td>
	<td>nypd_mv_collision_analysis/4.py</td>
</tr>        
</table>

Refer `docs/query_details.docx` for pre-processing steps.

Data Set References
==============================================================================================

| Dataset | Name | Url |
| ------- | ------------------------------ | -- |
| 1. |  Dataset for word count: |  http://www.gradesaver.com/divine-comedy-paradiso/e-text/canto-1 |
| 2. |  National Names dataset: |  https://www.kaggle.com/kaggle/us-baby-names    |
| 3. |  NYPD Motor Vehicle collision dataset |  https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95  |

    
Setup
==============================================================================================
The code was tested on standalone deployment of Apache Spark 2.1.0 on Windows 7 operating system.
## Softwares Required 
1. Java: http://www.oracle.com/technetwork/java/javase/downloads/index-jsp-138363.html
2. Spark: http://spark.apache.org/downloads.html
3. Scala: http://www.scala-lang.org/download/2.10.5.html
4. Python: https://www.python.org/download/releases/2.7/
5. sbt: http://www.scala-sbt.org/download.html
6. winutils: https://github.com/steveloughran/winutils/blob/master/hadoop-2.6.0/bin/winutils.exe
7. Anaconda: https://www.continuum.io/downloads#windows


## Installation instructions
Set the below Path Variables:

|   |  Variable                          | Path               |
| -- |---------------------------------- | ----------------------------- |
| 1. |  JAVA_HOME    | C:\Program Files\Java\jdk1.7.0_79                  |     
| 2. | SCALA_HOME    | C:\Program Files\scala                            |
| 3. | SBT_HOME    | C:\Program Files\sbt                                |
| 4. | SPARK_HOME    | C:\spark-2.1.0-bin-hadoop2.7\                        |
| 5. | HADOOP_HOME    | E:\winutils
| 6. | PATH        | %SCALA_HOME%\bin;%SBT_HOME%\bin;%SPARK_HOME %\bin;%JAVA_HOME%\bin;%HADOOP_HOME%\bin |



## Operating Instructions

1. Open Jupyter by running command "jupyter notebook" in command prompt
2. Download and get above datasets. Update the path of dataset file in .py files.
3. Copy *.py from /repo/ and run it in Jupyter

        

Observations
==============================================================================================
|   |  DATASET        |    DATASET SIZE                   |    PERFORMANCE                        |
| -- |------------------- | ------------------------------------ | ----------------------------------------- |
| 1. | WordCountData.txt    | WordCountData.txt 60KB,~1500 Lines   | Counted all words in 27 milli-seconds.    |
| 2. | NationalNames.csv    | 42MB,     ~2 Million Records           | Average Query Time: 11 msec,For 4 queries |
| 3. | NYPD_Motor_Vehicle_Collisions.csv    | 176 MB, ~1 Million Records    | Average Query Time: 22 msec  For 4 queries |      
   
