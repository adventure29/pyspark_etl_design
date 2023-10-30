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
put("Provide the adverse event Source URL: ")
  print("Reading Nested JSON File ... ")
  df = read_json_nested(df)
  read_nested_json_flag = False
  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      read_nested_json_flag = True
    elif isinstance(df.schema[column_name].dataType, StructType):
      read_nested_json_flag = True
 
