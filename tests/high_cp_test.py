import sys
import datetime
import unittest
from pyspark.sql import SparkSession
from src.etl import highest_closing_price_per_year
from pyspark.testing.utils import assertDataFrameEqual
sys.path.append('/home/batteringram/Desktop/Projects/pyspark-stock-data')


class HighestClosingPriceYearYear(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
           .appName('avg-cp-test') \
           .master('local[*]') \
           .getOrCreate()
        
    def test_highest_closing_price_per_year(self):

        test_df = self.spark.createDataFrame([
            {'date': datetime.date(year=2023, month=1, day=23), 'close': 2.0, 'open': 1.0},
            {'date': datetime.date(year=2023, month=1, day=24), 'close': 1.0, 'open': 2.0},
        ])

        expected_df = self.spark.createDataFrame([
            {'date': datetime.date(year=2023, month=1, day=23), 'close': 2.0, 'open': 1.0},
        ])

        result_df = highest_closing_price_per_year(test_df)
        assertDataFrameEqual(result_df, expected_df)

if __name__ == '__main__':
    unittest.main()

