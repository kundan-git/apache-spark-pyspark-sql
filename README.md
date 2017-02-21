# apache-spark-pyspark-sql

==============================================================================================
Softwares Required 
==============================================================================================
1. Java: http://www.oracle.com/technetwork/java/javase/downloads/index-jsp-138363.html
2. Spark: http://spark.apache.org/downloads.html
3. Scala: http://www.scala-lang.org/download/2.10.5.html
4. Python: https://www.python.org/download/releases/2.7/
5. sbt: http://www.scala-sbt.org/download.html
6. winutils: https://github.com/steveloughran/winutils/blob/master/hadoop-2.6.0/bin/winutils.exe
7. Anaconda: https://www.continuum.io/downloads#windows

==============================================================================================
Installation instructions
==============================================================================================
Set the below Path Variables:

1. JAVA_HOME	C:\Program Files\Java\jdk1.7.0_79
2. SCALA_HOME	C:\Program Files\scala
3. SBT_HOME	C:\Program Files\sbt
4. SPARK_HOME	C:\spark-2.1.0-bin-hadoop2.7\
5. HADOOP_HOME	E:\winutils
6. PATH	%SCALA_HOME%\bin;%SBT_HOME%\bin;%SPARK_HOME %\bin;%JAVA_HOME%\bin;%HADOOP_HOME%\bin

==============================================================================================
Operating instructions
==============================================================================================
1. Open Jupyter by running command "jupyter notebook" in command prompt
2. Copy .py from /repo/ and run it in Jupyter

==============================================================================================
Sample Applications and outputs
==============================================================================================
   |  Dataset                           | Source Folder                |
---|------------------------------------|------------------------------|
1. |  Word Count Application and output	|  word_count                  |
2. |  Analysis on National Names dataset|  national_names_analysis     |
3. |  Analysis on NYPD Motor Vehicle collision dataset	|  nypd_mv_collision_analysis  |
 		        

==============================================================================================
Data Set References
==============================================================================================
   |  Dataset                           | Source                 |
---|------------------------------------|------------------------------|
1. |  Dataset for word count:	|  http://www.gradesaver.com/divine-comedy-paradiso/e-text/canto-1                  |
2. |  National Names dataset:|  https://www.kaggle.com/kaggle/us-baby-names    |
3. |  NYPD Motor Vehicle collision dataset	|  https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95  |

		
==============================================================================================
Observations
==============================================================================================
   |  DATASET		|	DATASET SIZE                   |	PERFORMANCE                        |
---|--------------------|--------------------------------------|-------------------------------------------|
1. | WordCountData.txt	| WordCountData.txt 60KB,~1500 Lines   | Counted all words in 27 milli-seconds.    |
2. | NationalNames.csv	| 42MB,	 ~2 Million Records	       | Average Query Time: 11 msec,For 4 queries |
3. | NYPD_Motor_Vehicle_Collisions.csv	| 176 MB, ~1 Million Records    | Average Query Time: 22 msec  For 4 queries|      
   
