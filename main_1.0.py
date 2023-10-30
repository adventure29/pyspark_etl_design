##Designing Pyspark ETL 
#Step 1: Installation of all the modules required and setting up the environment using Google Colab
#Step 2: Reading the Source data -- Extracting the Source data into dataframe(df)
#step 3: Performing some transformation to view data into some structured form
#step 4: Loading data from df to target (Gooogle Colab Drive)

#------------------------------------------------------------------------------------------------------------------------
##Step 1 -- 
#Spark is written in the Scala programming language and requires the Java Virtual Machine (JVM) to run. Therefore, our first task is to download Java.
!apt-get install openjdk-8-jdk-headless -qq 
#Next, we will install Apache Spark 3.0.1 with Hadoop 2.7 from here.
!wget https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
#Now, we just need to unzip that folder.
!tar -xvzf spark-3.0.0-bin-hadoop2.7.tgz
#one last thing that we need to install and that is the findspark library. It will locate Spark on the system and import it as a regular library.
!pip install findspark
#This will enable us to run Pyspark in the Colab environment.
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.0.0-bin-hadoop2.7"
#We need to locate Spark in the system
import findspark
findspark.init()
#------------------------------------------------------------------------------------------------------------------------
# Step 2 - Extraction of Source data based on the source url we parse -- differs based on factor blocker
##Creates a dataframe by reading open api url - source url
# Read from HTTP link:
import pyspark
from pyspark.sql import *
from urllib.request import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Get Adverse Events data").getOrCreate()

#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"Tumor+Necrosis"&limit=1
#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"Thiazide+Diuretic"&limit=1
#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"nonsteroidal+anti-inflammatory+drug"&limit=1

#sourceURL = 'https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"nonsteroidal+anti-inflammatory+drug%22&limit=1"

sourceURL = input("Provide the adverse event Source URL: ")
#read the content of source - open API
httpData = urlopen(sourceURL).read().decode('utf-8')
# convert into RDD
rdd = spark.sparkContext.parallelize([httpData])

# create a Dataframe -- Extracting data and storing it into df dataframe
df = spark.read.json(rdd)
df.printSchema()
df.show()
#------------------------------------------------------------------------------------------------------------------------
#step 3: Performing some transformation to view data into some structured form
#Requirement: to extract specific fields related to adverse events
from pyspark.sql.functions import *
from pyspark.sql.types import *

#method to read content of open api file we read
def read_json_nested(df):
    #Step 1: Defining column as an empty list
    column_list = []
    # for loop starts to find type of column type of dataframe
    print("for loop starts")
    for column_name in df.schema.names:
        # Checking column type is ArrayType
        if isinstance(df.schema[column_name].dataType, ArrayType):
            print("Inside isinstance loop of ArrayType: " + column_name)
            df = df.withColumn(column_name, explode(column_name).alias(column_name))
            column_list.append(column_name)

        elif isinstance(df.schema[column_name].dataType, StructType):
            print("Inside isinstance loop of StructType: " + column_name)
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)

    # Selecting columns using column_list from dataframe: df
    df = df.select(column_list)
    return df
  read_nested_json_flag = True
  ##While Loop Starts
  while read_nested_json_flag:
    print("Reading Nested JSON File ... ")
    df = read_json_nested(df)
    read_nested_json_flag = False
    for column_name in df.schema.names:
      if isinstance(df.schema[column_name].dataType, ArrayType):
        read_nested_json_flag = True
      elif isinstance(df.schema[column_name].dataType, StructType):
        read_nested_json_flag = True
  df.show(2, False)
  #------------------------------------------------------------------------------------------------------------------------
  # Step 4: Loading data into some destination path -- in our case its google colab drive
  df.coalesce(1).write.mode("overwrite").csv("/content/drive/MyDrive/Adverse_Events_Data/adverse_event_data.csv", header=True)

