{
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/adventure29/pyspark_etl_design/blob/main/adverse_events_data_main.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "##Designing Pyspark ETL\n",
        "#Step 1: Installation of all the modules required and setting up the environment using Google Colab\n",
        "#Step 2: Reading the Source data -- Extracting the Source data into dataframe(df)\n",
        "#step 3: Performing some transformation to view data into some structured form\n",
        "#step 4: Loading data from df to target (Gooogle Colab Drive)"
      ],
      "metadata": {
        "id": "r35h3kSPlDEn"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "##Step 1 --\n",
        "#Spark is written in the Scala programming language and requires the Java Virtual Machine (JVM) to run. Therefore, our first task is to download Java.\n",
        "!apt-get install openjdk-8-jdk-headless -qq\n",
        "#Next, we will install Apache Spark 3.0.1 with Hadoop 2.7 from here.\n",
        "!wget https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz\n",
        "#Now, we just need to unzip that folder.\n",
        "!tar -xvzf spark-3.0.0-bin-hadoop2.7.tgz\n",
        "#one last thing that we need to install and that is the findspark library. It will locate Spark on the system and import it as a regular library.\n",
        "!pip install findspark\n",
        "#This will enable us to run Pyspark in the Colab environment.\n",
        "import os\n",
        "os.environ[\"JAVA_HOME\"] = \"/usr/lib/jvm/java-8-openjdk-amd64\"\n",
        "os.environ[\"SPARK_HOME\"] = \"/content/spark-3.0.0-bin-hadoop2.7\"\n",
        "#We need to locate Spark in the system\n",
        "import findspark\n",
        "findspark.init()"
      ],
      "metadata": {
        "id": "3NT-qNawK0Ze"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Step 2 - Extraction of Source data based on the source url we parse -- differs based on factor blocker\n",
        "##Creates a dataframe by reading open api url - source url\n",
        "# Read from HTTP link:\n",
        "import pyspark\n",
        "from pyspark.sql import *\n",
        "from urllib.request import *\n",
        "from pyspark.sql.functions import *\n",
        "from pyspark.sql.types import *\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Get Adverse Events data\").getOrCreate()\n",
        "\n",
        "#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:\"Tumor+Necrosis\"&limit=1\n",
        "#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:\"Thiazide+Diuretic\"&limit=1\n",
        "#https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:\"nonsteroidal+anti-inflammatory+drug\"&limit=1\n",
        "\n",
        "#sourceURL = 'https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:\"nonsteroidal+anti-inflammatory+drug%22&limit=1\"\n",
        "\n",
        "sourceURL = input(\"Provide the adverse event Source URL: \")\n",
        "#read the content of source - open API\n",
        "httpData = urlopen(sourceURL).read().decode('utf-8')\n",
        "# convert into RDD\n",
        "rdd = spark.sparkContext.parallelize([httpData])\n",
        "\n",
        "# create a Dataframe -- Extracting data and storing it into df dataframe\n",
        "df = spark.read.json(rdd)\n",
        "df.printSchema()\n",
        "df.show()"
      ],
      "metadata": {
        "id": "uUMlWMn5OKiQ"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "df.show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "CGf7JVTyuDza",
        "outputId": "21bd1f13-9edf-46b9-e3f7-3a4f9ed547e0"
      },
      "execution_count": 41,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+--------------------+\n",
            "|                meta|             results|\n",
            "+--------------------+--------------------+\n",
            "|[Do not rely on o...|[[AR-AMGEN-ARGSP2...|\n",
            "+--------------------+--------------------+\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "#step 3: Performing some transformation to view data into some structured form\n",
        "#Requirement: to extract specific fields related to adverse events\n",
        "from pyspark.sql.functions import *\n",
        "from pyspark.sql.types import *\n",
        "\n",
        "#method to read content of open api file we read\n",
        "def read_json_nested(df):\n",
        "    #Step 1: Defining column as an empty list\n",
        "    column_list = []\n",
        "    # for loop starts to find type of column type of dataframe\n",
        "    print(\"for loop starts\")\n",
        "    for column_name in df.schema.names:\n",
        "        # Checking column type is ArrayType\n",
        "        if isinstance(df.schema[column_name].dataType, ArrayType):\n",
        "            print(\"Inside isinstance loop of ArrayType: \" + column_name)\n",
        "            df = df.withColumn(column_name, explode(column_name).alias(column_name))\n",
        "            column_list.append(column_name)\n",
        "\n",
        "        elif isinstance(df.schema[column_name].dataType, StructType):\n",
        "            print(\"Inside isinstance loop of StructType: \" + column_name)\n",
        "            for field in df.schema[column_name].dataType.fields:\n",
        "                column_list.append(col(column_name + \".\" + field.name).alias(column_name + \"_\" + field.name))\n",
        "        else:\n",
        "            column_list.append(column_name)\n",
        "\n",
        "    # Selecting columns using column_list from dataframe: df\n",
        "    df = df.select(column_list)\n",
        "    return df\n",
        "\n",
        "\n",
        "### Main Program starts\n",
        "if __name__ == \"__main__\":\n",
        "\n",
        "  read_nested_json_flag = True\n",
        "  ##While Loop Starts\n",
        "  while read_nested_json_flag:\n",
        "    print(\"Reading Nested JSON File ... \")\n",
        "    df = read_json_nested(df)\n",
        "    read_nested_json_flag = False\n",
        "    for column_name in df.schema.names:\n",
        "      if isinstance(df.schema[column_name].dataType, ArrayType):\n",
        "        read_nested_json_flag = True\n",
        "      elif isinstance(df.schema[column_name].dataType, StructType):\n",
        "        read_nested_json_flag = True\n",
        "  df.show(2, False)\n",
        "  # Step 4: Loading data into some destination path -- in our case its google colab drive\n",
        "  df.coalesce(1).write.mode(\"overwrite\").csv(\"/content/drive/MyDrive/Adverse_Events_Data/adverse_event_data.csv\", header=True)\n"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "jQ-iGUkNsl-0",
        "outputId": "d7073f70-edf5-475c-fced-64db8ab6abb0"
      },
      "execution_count": 53,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Reading Nested JSON File ... \n",
            "for loop starts\n",
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+-----------------------------+------------------+-----------------+------------------+---------------------------+------------------------+-----------------+-------------------------------+--------------------+-------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------------+--------------------------------------------+-----------------------------------+-----------------------------------+-------------------------------------------------+-----------------------------------------------+-------------------------------------------+----------------------------------+----------------------------------------+--------------------------------------------+--------------------------------------------+-------------------------------------+-----------------------------------------------+---------------------------------------+-----------------------------------------+----------------------------------------------+--------------------------------+----------------------------------------+--------------------------------------------+------------------------------------------------------+----------------------------------------+-----------------------------------------+----------------------------------+----------------------------------+------------------------------------+---------------------------------------+-------------------------------------------+---------------------------------+-------------------------------+-----------------------------------+--------------------------+-----------------------------------------+------------------------------------------------+----------------------------------------+-----------------------------------+-------------------------------------+----------------------------+-------------------+-------------------------+-------------------+-------------------------+-------------------------------------+-----------------------------+-------------------------------------+---------------------------------------+------------------+----------------------+---------------------------+---------------------------------+-------------------------+---------------+------------------------+------------------------+------------------------------+\n",
            "|meta_disclaimer                                                                                                                                                                                                                                                         |meta_last_updated|meta_license                 |meta_results_limit|meta_results_skip|meta_results_total|meta_terms                 |results_companynumb     |results_duplicate|results_fulfillexpeditecriteria|results_occurcountry|results_patient_drug_actiondrug|results_patient_drug_drugadministrationroute|results_patient_drug_drugauthorizationnumb|results_patient_drug_drugcharacterization|results_patient_drug_drugdosageform         |results_patient_drug_drugdosagetext|results_patient_drug_drugindication|results_patient_drug_drugintervaldosagedefinition|results_patient_drug_drugintervaldosageunitnumb|results_patient_drug_drugseparatedosagenumb|results_patient_drug_drugstartdate|results_patient_drug_drugstartdateformat|results_patient_drug_drugstructuredosagenumb|results_patient_drug_drugstructuredosageunit|results_patient_drug_medicinalproduct|results_patient_drug_openfda_application_number|results_patient_drug_openfda_brand_name|results_patient_drug_openfda_generic_name|results_patient_drug_openfda_manufacturer_name|results_patient_drug_openfda_nui|results_patient_drug_openfda_package_ndc|results_patient_drug_openfda_pharm_class_epc|results_patient_drug_openfda_pharm_class_moa          |results_patient_drug_openfda_product_ndc|results_patient_drug_openfda_product_type|results_patient_drug_openfda_route|results_patient_drug_openfda_rxcui|results_patient_drug_openfda_spl_id |results_patient_drug_openfda_spl_set_id|results_patient_drug_openfda_substance_name|results_patient_drug_openfda_unii|results_patient_patientonsetage|results_patient_patientonsetageunit|results_patient_patientsex|results_patient_reaction_reactionmeddrapt|results_patient_reaction_reactionmeddraversionpt|results_patient_reaction_reactionoutcome|results_primarysource_qualification|results_primarysource_reportercountry|results_primarysourcecountry|results_receiptdate|results_receiptdateformat|results_receivedate|results_receivedateformat|results_receiver_receiverorganization|results_receiver_receivertype|results_reportduplicate_duplicatenumb|results_reportduplicate_duplicatesource|results_reporttype|results_safetyreportid|results_safetyreportversion|results_sender_senderorganization|results_sender_sendertype|results_serious|results_seriousnessother|results_transmissiondate|results_transmissiondateformat|\n",
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+-----------------------------+------------------+-----------------+------------------+---------------------------+------------------------+-----------------+-------------------------------+--------------------+-------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------------+--------------------------------------------+-----------------------------------+-----------------------------------+-------------------------------------------------+-----------------------------------------------+-------------------------------------------+----------------------------------+----------------------------------------+--------------------------------------------+--------------------------------------------+-------------------------------------+-----------------------------------------------+---------------------------------------+-----------------------------------------+----------------------------------------------+--------------------------------+----------------------------------------+--------------------------------------------+------------------------------------------------------+----------------------------------------+-----------------------------------------+----------------------------------+----------------------------------+------------------------------------+---------------------------------------+-------------------------------------------+---------------------------------+-------------------------------+-----------------------------------+--------------------------+-----------------------------------------+------------------------------------------------+----------------------------------------+-----------------------------------+-------------------------------------+----------------------------+-------------------+-------------------------+-------------------+-------------------------+-------------------------------------+-----------------------------+-------------------------------------+---------------------------------------+------------------+----------------------+---------------------------+---------------------------------+-------------------------+---------------+------------------------+------------------------+------------------------------+\n",
            "|Do not rely on openFDA to make decisions regarding medical care. While we make every effort to ensure that data is accurate, you should assume all results are unvalidated. We may limit or otherwise restrict your access to the API in line with our Terms of Service.|2023-08-02       |https://open.fda.gov/license/|1                 |0                |859177            |https://open.fda.gov/terms/|AR-AMGEN-ARGSP2014016868|1                |1                              |AR                  |4                              |065                                         |103795                                    |1                                        |Solution for injection in pre-filled syringe|50 MG, WEEKLY                      |RHEUMATOID ARTHRITIS               |803                                              |1                                              |1                                          |20140217                          |102                                     |50                                          |003                                         |ENBREL                               |BLA103795                                      |ENBREL                                 |ETANERCEPT                               |Immunex Corporation                           |N0000175610                     |58406-435-01                            |Tumor Necrosis Factor Blocker [EPC]         |Tumor Necrosis Factor Receptor Blocking Activity [MoA]|58406-010                               |HUMAN PRESCRIPTION DRUG                  |SUBCUTANEOUS                      |253014                            |c7f12928-1a00-4b83-a206-18e22ef55cf9|a002b40c-097d-47a5-957f-7a7b1807af7f   |ETANERCEPT                                 |OP401G7OJC                       |69                             |801                                |2                         |Visual impairment                        |17.0                                            |1                                       |5                                  |AR                                   |AR                          |20140312           |102                      |20140312           |102                      |FDA                                  |6                            |AR-AMGEN-ARGSP2014016868             |AMGEN                                  |1                 |10003765              |1                          |FDA-Public Use                   |2                        |1              |1                       |20141002                |102                           |\n",
            "|Do not rely on openFDA to make decisions regarding medical care. While we make every effort to ensure that data is accurate, you should assume all results are unvalidated. We may limit or otherwise restrict your access to the API in line with our Terms of Service.|2023-08-02       |https://open.fda.gov/license/|1                 |0                |859177            |https://open.fda.gov/terms/|AR-AMGEN-ARGSP2014016868|1                |1                              |AR                  |4                              |065                                         |103795                                    |1                                        |Solution for injection in pre-filled syringe|50 MG, WEEKLY                      |RHEUMATOID ARTHRITIS               |803                                              |1                                              |1                                          |20140217                          |102                                     |50                                          |003                                         |ENBREL                               |BLA103795                                      |ENBREL                                 |ETANERCEPT                               |Immunex Corporation                           |N0000175610                     |58406-435-01                            |Tumor Necrosis Factor Blocker [EPC]         |Tumor Necrosis Factor Receptor Blocking Activity [MoA]|58406-010                               |HUMAN PRESCRIPTION DRUG                  |SUBCUTANEOUS                      |261105                            |c7f12928-1a00-4b83-a206-18e22ef55cf9|a002b40c-097d-47a5-957f-7a7b1807af7f   |ETANERCEPT                                 |OP401G7OJC                       |69                             |801                                |2                         |Visual impairment                        |17.0                                            |1                                       |5                                  |AR                                   |AR                          |20140312           |102                      |20140312           |102                      |FDA                                  |6                            |AR-AMGEN-ARGSP2014016868             |AMGEN                                  |1                 |10003765              |1                          |FDA-Public Use                   |2                        |1              |1                       |20141002                |102                           |\n",
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+-----------------------------+------------------+-----------------+------------------+---------------------------+------------------------+-----------------+-------------------------------+--------------------+-------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------------+--------------------------------------------+-----------------------------------+-----------------------------------+-------------------------------------------------+-----------------------------------------------+-------------------------------------------+----------------------------------+----------------------------------------+--------------------------------------------+--------------------------------------------+-------------------------------------+-----------------------------------------------+---------------------------------------+-----------------------------------------+----------------------------------------------+--------------------------------+----------------------------------------+--------------------------------------------+------------------------------------------------------+----------------------------------------+-----------------------------------------+----------------------------------+----------------------------------+------------------------------------+---------------------------------------+-------------------------------------------+---------------------------------+-------------------------------+-----------------------------------+--------------------------+-----------------------------------------+------------------------------------------------+----------------------------------------+-----------------------------------+-------------------------------------+----------------------------+-------------------+-------------------------+-------------------+-------------------------+-------------------------------------+-----------------------------+-------------------------------------+---------------------------------------+------------------+----------------------+---------------------------+---------------------------------+-------------------------+---------------+------------------------+------------------------+------------------------------+\n",
            "only showing top 2 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df.rdd.getNumPartitions()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "jOvgnahRKxFR",
        "outputId": "5f5c7950-dd29-46d0-bbe9-8923b50ed13f"
      },
      "execution_count": 54,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "2"
            ]
          },
          "metadata": {},
          "execution_count": 54
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [],
      "metadata": {
        "id": "gq0rK1LryDhS"
      },
      "execution_count": null,
      "outputs": []
    }
  ],
  "metadata": {
    "colab": {
      "provenance": [],
      "mount_file_id": "1hRVD915D9XGJmDybgMikxuQDwkl_65-k",
      "authorship_tag": "ABX9TyNd7psf7uMZ4N3CiDpFmqWD",
      "include_colab_link": true
    },
    "kernelspec": {
      "display_name": "Python 3",
      "name": "python3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "nbformat": 4,
  "nbformat_minor": 0
}
