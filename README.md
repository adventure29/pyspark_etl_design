# pyspark_etl_design
Performing ETL using pyspark in Google Colab Application

Pyspark ETL Design
Environment Used: Google Colaboratory Studio
Extract: imported and using url libraries to read the source url(open api request url) and stored the data to df dataframe as raw data itself(spark.read)(source.py)
Transformation: 
    -- performing and applying explode function to divide the array type column names, directly dividing the struct type datatype and storing the columns to particular list( appending all column names to list variable)(main.py)
    -- Creating a new dataframe and selecting all the column name data
Load: writing the dataframe data into parquet(format(parquet)) or csv file(format(csv)) or creating a data using(saveAsTable)(main.py)

Structure of Scripts in repo:
Prerequesites.txt
main.py(calling source.py inside this python script)
source.py

MakeFile -- has commands to execute the above scripts to perform ETL
Q&A -- has all the solutions to questions
