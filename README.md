# Introduction
The Goal of this Project is to learn about **Data Engineering** my main goal is to build a simple ETL pipeline that:
* _Extracts_ the latest chess games I've played from the chess.com Api  daily
* _Transforms_ them into a useful model using **Apache Spark (PySpark)**  such as the Kimball Model
* makes them _Loadable_ from a Analytics Tool such as **Jupyter Lab**

# Getting Started

 To start up the Application refer to [SETUP.md](SETUP.md)

# Learnings

Things I learned about whilst developing this Project:

* Apache Spark (PySpark)
* Jupyter Notebook 
* [Kimball Modelling (related to Data Warehouses)](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
    * Dimension Tables -> Dimensions
        * In Star Model has pure denormalized Dimension Tables
        * Snow Flake Design can have normalized Dimension Tables 
    * Fact Tables -> Measures
* Apache Airflow
   * DAGs
   * Providers
   * Configurations
   * Connecting to Spark
* Batch Processing vs Stream Processing
   * Differences in MapReduce & Spark
      * why MapReduce is slower (More IO)
      * why Spark is faster (can Process alot in Memory) & Ability to do Stream Processing
