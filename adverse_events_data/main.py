
import pyspark
from pyspark.sql import *
from urllib.request import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import source

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

def main():
	spark = SparkSession.builder.appName("Get Adverse Events data").getOrCreate() 
	read_nested_json_flag = True
  ##While Loop Starts
	while read_nested_json_flag:
  	sourceURL = input("Provide the adverse event Source URL: ")
  	source.source_dataframe(sourceURL)
  	print("Reading Nested JSON File ... ")
  	df = read_json_nested(df)
  	read_nested_json_flag = False
  	for column_name in df.schema.names:
    	if isinstance(df.schema[column_name].dataType, ArrayType):
      	read_nested_json_flag = True
    elif isinstance(df.schema[column_name].dataType, StructType):
      read_nested_json_flag = True
 
def main():
    # pull data from Reddit
    data = extract()
    # transform reddit data
    transformed_data = transform(data)
    # load data into database
    load(transformed_data)
    
if __name__ == '__main__':
    main()