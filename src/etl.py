from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when
import pyspark.sql.functions as F


def create_spark_session():
    # Creating a Spark Session
    spark = SparkSession.builder \
        .appName('pyspark-stock-data') \
        .master('local[*]') \
        .config('spark.jars', '/home/batteringram/Downloads/postgresql-42.5.0.jar') \
        .getOrCreate()
    return spark
    

def check_spark():
    # Checking our Spark version here
    spark = create_spark_session()
    print('Spark Session Version: ', spark.version)


def main():
    # Reading CSV file and getting schema
    spark = create_spark_session()
    df = spark.read.csv('data/NVDA.csv', header = True, inferSchema = True)
    df.printSchema()
    return df


def checking_year_span():
    # This function gets new column with time spans telling in which time period it is
    year_span = when(F.col('Date').startswith('1999'), "Before 2000") \
                .when((F.col('Date') >= '2000-01-03') & (F.col('Date') <= "2010-12-31"), 'In Between 2000 & 2010') \
                .when((F.col('Date') >= '2010-12-31') & (F.col('Date') <= "2019-12-31"), 'In Between 2010 & 2020') \
                .otherwise('On & After 2020')
    return df.withColumn('Year Flag', year_span).show()
    


def average_closing_price_per_year(df):
    # Average closing price for each year using GroupBys
    acp_df = df.withColumn('Year', F.year(F.col('Date')))
    result = acp_df.groupBy('Year') \
                   .agg(F.avg('Close').alias('Average Closing Price')) \
                   .orderBy('Year') \
                   .show()
    return result


def highest_closing_price_per_year(df):
    # Highest Closing Price
    window_function = Window.partitionBy(F.year(F.col('Date'))).orderBy(F.col('Close').desc())

    return df.withColumn('rank', F.row_number().over(window_function)) \
      .filter(F.col('rank') == 1) \
      .drop('rank') \
      .show()


def write_to_postgres(df):
    # Writing our data to a Postgres table
    df.write.format('jdbc') \
              .option('url', 'jdbc:postgresql://localhost:5432/stock-data') \
              .option('driver', 'org.postgresql.Driver') \
              .option('dbtable', 'nvda_stocks') \
              .option('user', 'postgres') \
              .option('password', 'postgres') \
              .option('header', 'true') \
              .option('inferSchema', 'true') \
              .save()



if __name__ == '__main__':
    create_spark_session()
    check_spark()
    df = main()
    # Perform Aggregations
    checking_year_span()
    average_closing_price_per_year(df)
    highest_closing_price_per_year(df)
    write_to_postgres(df)

