# Project 4: Apache Spark & Data Lake
[![Project passed](https://img.shields.io/badge/project-passed-success.svg)](https://img.shields.io/badge/project-passed-success.svg)

## Summary
* [Preamble](#Preamble)
* [Spark process](#Spark-process)
* [Project structure](#Project-structure)
--------------------------------------------

#### Preamble
Data source is provided by one public S3 buckets. This bucket contains 
info about songs, artists and actions done by users (which song are listening, etc..). The objects contained in the bucket are JSON files. The entire elaboration is done with Apache Spark

--------------------------------------------
#### Spark process

The ETL job processes the song files then the log files. The song files are listed and iterated over entering relevant information in the artists and the song folders in parquet.
The log files are filtered by the NextSong action. The subsequent dataset is then processed to extract the date , time , year etc. fields and records are then appropriately entered into the time, users and songplays folders in parquet for analysis.

-------------------------
#### Project structure
This is the project structure, if the bullet contains ``/`` <br>
means that the resource is a folder:

* <b> /data </b> - A folder that cointains two zip files, helpful for data exploration
* <b> etl.py </b> - The ETL engine done with Spark, data normalization and parquet file writing.
* <b> dl.cfg </b> - Configuration file that contains info about AWS credentials
