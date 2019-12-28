# Data-lake-with-Spark
This repository contains the code of ETL pipeline for a data lake hosted on AWS S3 that is processed into analytics tables using Apache Spark.

## Project Description

A startup wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a data engineer I converted the JSON logs into `.parquet` files containing the data in the form of a relational database schema by utilizing SparkSQL. These `.parquet` files can now be loaded for their analytics work through Spark or as Pandas Dataframe in python or loaded into AWS Athena for interactive analysis.

## Project Structure

* `data` -  folder containing all the JSON logs that contain data about user activity on app and metadata about songs.
* `dl.cfg` -  Configuration file containing information about AWS Credentials to access S3.
* `etl.py` - reads and processes all JSON logs that are stored in S3 bucket using Spark and writes the results in `.parquet` files back into the S3 bucket.
* `README.md` - Provides a summary of the project and discussions on the data modelling.
* `Resource` - Folder containing images that were used in the README.

## Choice of Database

The reason that I chose a relational database management system for this project are as follows:

* The volume of data the startup is dealing with is quite a small dataset.
* The data from the music streaming app is structured as we know how the key-value pairs are stored inside the JSON logs and what are their data types. 
* Easier to change to business requirements and flexibility in queries as the analytics team would want to perform ad-hoc queries to get a better understanding of the songs that different users are listening to.
* The analytics team would want to be able to do aggregations and analytics on the data.
* The ability to do JOINs would be very useful here due to the way data is getting logged in JSON files. Please see Entity relationship diagram below. Even though JOINs are slow, due to the small size of the dataset this shouldn't be a problem.

## Entity Relationship Diagram (ERD)

![Image](https://github.com/arahman5/Data-Modeling-with-PostgreSQL/blob/master/resource/ERD.PNG)

The above represents the entity relationship diagram which shows how the different keys within the JSON logs can be connected together in a star schema. **songplays** is the fact table showing foreign keys that connect this table to all the other dimension tables. **users, time, songs, artists** are all dimension tables, each containing a primary key unique to each table. A star schema was chosen for this database design because of the following reasons:

* Star schema supports denormalization of the data, which would be quite useful in this analytics case as this will allow the analytics team to execute simple queries and fast aggregations on the data. 
* Star schema supports one to one mapping, which is easy to implement and works very well in this case due to the small number of keys within the JSON logs. 

## ETL Pipeline

Please see list of key steps that happens in the ETL Pipeline below:

* The logs for song metadata and event data about user activity in the app are both read from S3 using Apache Spark.
* A view is then created using Spark for loading information from the logs into a temporary table.
* The relevant columns for each table are then chosen by utilizing the SparkSQL library and also ensuring that there are no duplicates, i.e. there is a Primary key in each table.
* This subset of selected column for each table are then written into `.parquet` format in a folder in S3 named appropriately after the name of the tables.

## Running the scripts

### Udacity Workspace

Firstly, fill in the `dl.cfg` file with your AWS Configuration details. I have removed mine due to obvious security issues. Secondly,
execute the below command in the terminal:

```python
python etl.py
```

### Locally (Only works if you have AWS S3 and EMR access)

*. Download `data` from this repo and upload the folders `song_data` and `log_data` with all of its contents in your S3 bucket.
*. Change the `input_file` and `output_file` paths in `etl.py` with your S3 bucket file path.
*. Update the AWS Configuration details in `dl.cfg` file with yours.
*. 

