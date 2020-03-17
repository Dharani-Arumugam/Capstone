
# Data Engineering Capstone Project

## Udacity Provided Project
The goal of udacity provided project is to create an ETL pipeline using I94 immigration data , city temperature data, airport-codes, us-city-demographics to form a database that is optimized for queries on immigration events. This database can be used to answer questions relating immigration behavior, destination temperature and the immigration rate with respect to race etc

The project follows the follow steps:
### Scope the Project and Gather Data
In this project, we will aggregate I94 immigration data by destination city to form our first dimension table. Next we will aggregate demographics data to find the race of immigrants in the demographics table. The three datasets will be joined on destination city to form the fact table. The final database is optimized to query on immigration events to determine if temperature affects the selection of destination cities. Spark will be used to process the data.
#### Describe and Gather Data
The I94 immigration data comes from the US National Tourism and Trade Office. It is provided in SAS7BDAT format which is a binary database storage format. Some relevant attributes include:

i94yr = 4 digit year
i94mon = numeric month
i94cit = 3 digit code of origin city
i94port = 3 character code of destination USA city
arrdate = arrival date in the USA
i94mode = 1 digit travel code
depdate = departure date from the USA
i94visa = reason for immigration

The temperature data comes from Kaggle. It is provided in csv format. Some relevant attributes include:

AverageTemperature = average temperature
City = city name
Country = country name
Latitude= latitude
Longitude = longitude
### Explore and Assess the Data
For the I94 immigration data, we want to drop all entries where the destination city code i94port is not a valid value (e.g., XXX, 99, etc) as described in I94_SAS_Labels_Description.SAS. For the temperature data, we want to drop all entries where AverageTemperature is NaN, then drop all entries with duplicate locations, and then add the i94port of the location in each entry.

Step 3: Define the Data Model
1. Immigration dimension table :
    cicid          - Identification - Primarykey
    i94yr          - 4 digit i94 year
    i94mon         - numeric month
    i94cit         - origin city
    i94res         - origin city
    i94port        - destination city
    arrdate        - arrival date
    i94mode        - mode
    i94addr        - address
    depdate        - departure date
    airline        - airlines
    fltno          - flight num
    i94bir         - birth year
    i94visa        - Type of visa
    gender         - gender
    visatype       - visatype
    stay           - num of days to stay

  2. demographics data :
  This data is aggregated to find the count of people by their race :
  3. temperature data :
  This shows the temperature of the country aggregated
  4. airport data :

####  Run ETL to Model the Data
The data is cleaned, extracted loaded into amazon s3 bucket as parquet files using apache Spark.

create an EMR cluster with spark to process the data.

Once the data is processed it will be stored in s3 buckets as parquet files.

#### Complete Project Write Up

Clearly state the rationale for the choice of tools and technologies for the project.
  Spark was chosen since it can easily handle multiple file formats (including SAS) containing large amounts of data. Spark  dataframes was chosen to process the large input files into dataframes and manipulated via standard SQL join operations to form additional tables.
  
Document the steps of the process.
   Create an EMR cluster in AWS
   Add the details in `dl.cfg` file
   Run `etl.py` which will load the processed data into s3 buckets

   The data should be updated monthly in conjunction with the current raw file format.

Write a description of how you would approach the problem differently under the following scenarios:

  1.The data was increased by 100x.
      If the data was increased by 100x, we would no longer process the data as a single batch job. We could perhaps do incremental updates using a tool such as Uber's Hudi. We could also consider moving Spark to cluster mode using a cluster manager such as Yarn.

  2.The data populates a dashboard that must be updated on a daily basis by 7am every day.
    If the data needs to populate a dashboard daily to meet an SLA then we could use a scheduling tool such as Airflow to run the ETL pipeline overnight.

  3.The database needed to be accessed by 100+ people.
    If the database needed to be accessed by 100+ people, we could consider publishing the parquet files to HDFS and giving read access to users that need it
