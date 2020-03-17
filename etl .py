import re
import os
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['KEY']     = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['SECRET']  = config['AWS']['AWS_SECRET_ACCESS_KEY']

input_immigration  = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
input_temperature  = '../../data2/GlobalLandTemperaturesByCity.csv'
input_demography   = 'us-cities-demographics.csv'
output_bucket      = 's3a://capstone-udacity'

date_format = "%Y-%m-%d"

def create_spark_session():
    'Create spark session'
    spark = SparkSession.builder.config("spark.jars.packages",
                                        "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")\
            .appName('capstone-Proj')\
            .enableHiveSupport()\
            .getOrCreate()
    return spark;
     
def process_etl_immigration(spark):
    'Process the Immigration data using spark to store it as a parquet file'
    re_obj = re.compile(r'\'(.*)\'.*\'(.*)\'')
    i94port_valid = {}
    
    with open('lookup/validports_i94.txt') as f:
        for line in f:
            match = re_obj.search(line)
            i94port_valid[match[1]]=[match[2]]
            
    immigration_df = spark.read.format("com.github.saurfang.sas.spark")\
                    .load(input_immigration)
    
    #Process the immigrant data who entered into US in Valid Port    
    immigration_df = immigration_df.filter(immigration_df.i94port.isin(list(i94port_valid.keys())))
    
    cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port',
            'arrdate', 'i94mode','i94addr','depdate' , 'airline', 'fltno',
            'i94bir', 'i94visa', 'gender','visatype']
    
    date_cols = ['arrdate', 'depdate']
    
    immigration_df = convert_sas_date(immigration_df, date_cols)
      
    # Create a new columns to store the length of the visitor stay in the US
    immigration_df = immigration_df.withColumn('stay', date_diff_udf(immigration_df.arrdate, immigration_df.depdate))
    immigration_df = cast_type(immigration_df, {'stay': IntegerType()})
    
    immigration_output = output_bucket + '/output/immigration.parquet'
    
    immigration_df.select(cols).write.mode("overwrite").partitionBy("i94yr","i94mon").parquet(immigration_output)
    
def convert_sas_date(df, cols):
    """
    Convert dates in the SAS datatype to a date in a string format YYYY-MM-DD
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        cols (:obj:`list`): List of columns in the SAS date format to be convert
    """
    for c in [c for c in cols if c in df.columns]:
        df = df.withColumn(c, convert_sas_udf(df[c]))
    return df

def date_diff(date1, date2):
    '''
    Calculates the difference in days between two dates
    '''
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, date_format)
        b = datetime.strptime(date2, date_format)
        delta = b - a
        return delta.days

def cast_type(df, cols):
    """
    Convert the types of the columns according to the configuration supplied in the cols dictionary in the format {"column_name": type}
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        cols (:obj:`dict`): Dictionary in the format of {"column_name": type} indicating what columns and types they should be converted to
    """
    for k,v in cols.items():
        if k in df.columns:
            df = df.withColumn(k, df[k].cast(v))
    return df

def etl_temperature_data(spark):
    'Process the temperature data using spark to store it as a parquet file'
    temperature_df = spark.read.csv(input_temperature)

    # Aggregates the dataset by Country and rename the name of new columns
    countries = temperature_df.groupby(["Country"]).agg({"AverageTemperature": "avg", "Latitude": "first", "Longitude": "first"})\
    .withColumnRenamed('avg(AverageTemperature)', 'Temperature')\
    .withColumnRenamed('first(Latitude)', 'Latitude')\
    .withColumnRenamed('first(Longitude)', 'Longitude')
    
    # Rename specific country names to match the I94CIT_I94RES lookup table when joining them
    change_countries = [("Country", "Congo (Democratic Republic Of The)", "Congo"), ("Country", "Côte D'Ivoire", "Ivory Coast")]
    countries = change_field_value_condition(countries, change_countries)
    countries = countries.withColumn('Country_Lower', lower(countries.Country))
    
    # Rename specific country names to match the demographics dataset when joining them
    change_res = [("I94CTRY", "BOSNIA-HERZEGOVINA", "BOSNIA AND HERZEGOVINA"), 
                  ("I94CTRY", "INVALID: CANADA", "CANADA"),
                  ("I94CTRY", "CHINA, PRC", "CHINA"),
                  ("I94CTRY", "GUINEA-BISSAU", "GUINEA BISSAU"),
                  ("I94CTRY", "INVALID: PUERTO RICO", "PUERTO RICO"),
                  ("I94CTRY", "INVALID: UNITED STATES", "UNITED STATES")]
    
    # Loads the lookup table I94CIT_I94RES
    res = spark.read.csv("lookup/I94CIT_I94RES.csv")
    
    res = cast_type(res, {"Code": IntegerType()})
    res = change_field_value_condition(res, change_res)
    res = res.withColumn('Country_Lower', lower(res.I94CTRY))
    # Join the two datasets to create the country dimmension table
    res = res.join(countries, res.Country_Lower == countries.Country_Lower, how="left")
    res = res.withColumn("Country", when(isnull(res["Country"]), capitalize_udf(res.I94CTRY)).otherwise(res["Country"]))   
    res = res.drop("I94CTRY", "Country_Lower")
    
    country_output = output_bucket + '/output/country.parquet'
    
    res.write.mode("overwrite").parquet(country_output)

def change_field_value_condition(df, change_list):
    '''
    Helper function used to rename column values based on condition.
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        change_list (:obj: `list`): List of tuples in the format (field, old value, new value)
    '''
    for field, old, new in change_list:
        df = df.withColumn(field, when(df[field] == old, new).otherwise(df[field]))
    return df
  
def etl_demographics_data(spark):
    'Process the demographics data using spark to store it as a parquet file'
    demographics_df = spark.read.csv(input_demographics)
    demographics_output = output_bucket + '/output/demographics.parquet'
    demographics_df.write.mode("overwrite").partitionBy("State Code").parquet(demographics_output)
    
     
    first_agg = {"Median Age": "first", "Male Population": "first", "Female Population": "first", 
                 "Total Population": "first", "Number of Veterans": "first", "Foreign-born": "first",
                 "Average Household Size": "first"}
    # First aggregation - City
    agg_df = demographics_df.groupby(["City", "State", "State Code"]).agg(first_agg)
    # Pivot Table to transform values of the column Race to different columns
    piv_df = demographics_df.groupBy(["City", "State", "State Code"]).pivot("Race").sum("Count")
    
    demographics_df = agg_df.join(other=piv_df, on=["City", "State", "State Code"], how="inner")\
                     .withColumnRenamed('first(Total Population)', 'TotalPopulation')\
                     .withColumnRenamed('first(Female Population)', 'FemalePopulation')\
                     .withColumnRenamed('first(Male Population)', 'MalePopulation')\
                     .withColumnRenamed('first(Median Age)', 'MedianAge')\
                     .withColumnRenamed('first(Number of Veterans)', 'NumberVeterans')\
                     .withColumnRenamed('first(Foreign-born)', 'ForeignBorn')\
                     .withColumnRenamed('first(Average Household Size)', 'AverageHouseholdSize')\
                     .withColumnRenamed('Hispanic or Latino', 'HispanicOrLatino')\
                     .withColumnRenamed('Black or African-American', 'BlackOrAfricanAmerican')\
                     .withColumnRenamed('American Indian and Alaska Native', 'AmericanIndianAndAlaskaNative')
    
    numeric_cols = ['TotalPopulation', 'FemalePopulation', 'MedianAge', 'NumberVeterans', 'ForeignBorn', 'MalePopulation',                           'AverageHouseholdSize','AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican',                                       'HispanicOrLatino', 'White']
    # Fill the null values with 0
    demographics_df = demographics_df.fillna(0, numeric_cols)
    
    
    demography_output = output_bucket + '/output/demographics.parquet'
    demographics_df.write.mode("overwrite").partitionBy("Country", "City").parquet(demography_output)

convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))
capitalize_udf = udf(lambda x: x if x is None else x.title())
date_diff_udf = udf(date_diff)

if __name__ == "__main__" :
    spark = create_spark_session()
    
    immigration  = process_etl_immigration(spark)
    demographics = etl_demographics_data(spark)
    country      = etl_temperature_data(spark)
