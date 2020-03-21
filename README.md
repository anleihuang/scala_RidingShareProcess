# Ride Share's N-day Retention Analysis

## Introduction
In order to get know more about the customers and to evalulate the impact of any implemented strategies, the first step is to understand how many new users register and among them, how many return at a later days. In other words, conduct the retention analysis is the first priority.

The concept of the retention rate is through comparing the number of the customers that remains at the end of the time period to the number of customers that exists at the beginning of the time period. For example, on December 26th, there are 5 customers, the next day (December 27th), 3 customer are remained (#1, #2 and #3), lost 2 customers are lost (#4 and #5) and there is 1 new customer (#6), the 1-Day retention rate for December 26th is 60% (3/5).
<img src="https://github.com/anleihuang/scala_RidingShareProcess/blob/master/docs/crr_example.png"  width="500" height="300">

More details about the retention analysis and the explanation can be found in the article [Calculate and Improve Customer Retention Rate](https://www.salesforce.com/products/service-cloud/best-practices/customer-retention-rate/)

In the project, we care about how many customers are back the next day, in three days and in a week. Therefore, 5 Spark jobs are performed on a daily basis in order to calculate the N-day retention rate. The 5 Spark Jobs are
- Calculate unique user till the day
- Back calculate 1-Day retention rate for yesterday
- Back calculate 3-Day retention rate for the date 3 days ago
- Back calculate 7-Day retention rate for the date a week ago
- Calculate the average duration of the usage every day


## Architecture
The full end-to-end ETL pipeline is designed on Google Cloud Platform which reads files from Google Cloud Storage, processes the data in Apache Spark and loads the processed results to Google Big Query. Apache Airflow is used to schedule the workflow on a daily basis.

<img src="https://github.com/anleihuang/scala_RidingShareProcess/blob/master/docs/infra.png"  width="600" height="300">

## Testing
Open a terminal and go to the repo folder (i.e. `cd /path/to/scala_RidingShareProcess`), run `sbt test`. It will run all the tests defined under `src/test/scala/`

## Implementation
- Open a terminal and go to the repo folder (i.e. `cd /path/to/scala_RidingShareProcess`)
- Compile the project jar through `sbt assembly`
- Upload the jar file to your desired folder on Google Cloud Storage
#### Apache Airflow orchetratrue will then take from here
- Clone the [airflow template repo](https://github.com/anleihuang/airflow_withJinjia)
- Modify the parameters such as `main_class`, `jars` and `args` along with the other parameters in `dag_config.yaml` and `dataproc-arflow.py.j2` 
- Follow the steps specified in README file to fulfil the end-to-end ETL pipeline for the riding share project

## Requirements
- java8
- scala 2.11
- sbt 1.2.8
- [scallop 3.1.2](https://github.com/scallop/scallop): Scala command line arguments parsing library
- [ScalaTest 3.0.1](http://www.scalatest.org/user_guide/using_scalatest_with_sbt): For performing tests for Scala
- [spark-testing-base 2.3.0_0.14.0](https://github.com/holdenk/spark-testing-base): For performing tests with Spark

## Technology stacks
- Google Cloud Platform
- Google Cloud Storage
- Apache Spark 2.3.0
- Apache Airflow 1.10
