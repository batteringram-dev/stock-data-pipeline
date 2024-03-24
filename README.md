# NVIDIA Stock Data Pipeline in PySpark

## Overview
This is a small project on stock data in Spark & Python. Transformations such as adding new columns, group bys, window functions, and test cases are written and applied to the data frame and pushed to the Postgres DB for further analysis.

## What does this pipeline do?
- Spark reads the dataset from the data folder and sets a schema
- Year Flag function will add a new column that will say in which period this row falls into
- Average Closing Price tells us the average closing price of each year from 1999.
- The highest Closing Price gives us the highest closing price of each year. Implemented using a Window Function and a test case has been written for it to check if our function is returning correct values.
- The data is pushed to Postgres DB using the JDBC connector.
