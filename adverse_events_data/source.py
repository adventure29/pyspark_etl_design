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
