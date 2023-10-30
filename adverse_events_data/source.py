# Step 2 - Extraction of Source data based on the source url we parse -- differs based on factor blocker
##Creates a dataframe by reading open api url - source url
# Read from HTTP link:

import pyspark
from pyspark.sql import *
from urllib.request import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def source_dataframe(sourceURL):

#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"Tumor+Necrosis"&limit=1
#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"Thiazide+Diuretic"&limit=1
#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"nonsteroidal+anti-inflammatory+drug"&limit=1

#sourceURL = 'https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"Thiazide+Diuretic"&limit=1"
#read the content of source - open API
	httpData = urlopen(sourceURL).read().decode('utf-8')
# convert into RDD
rdd = spark.sparkContext.parallelize([httpData])

# create a Dataframe -- Extracting data and storing it into df dataframe
df = spark.read.json(rdd,multiLine=True)
df.printSchema()
df.show()
df.count()

#step 3: Performing some transformation to view data into some structured form
#Requirement: to extract specific fields related to adverse events


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
  sourceURL = input("Provide the adverse event Source URL: ")
  print("Reading Nested JSON File ... ")
  df = read_json_nested(df)
  read_nested_json_flag = False
  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      read_nested_json_flag = True
    elif isinstance(df.schema[column_name].dataType, StructType):
      read_nested_json_flag = True
 