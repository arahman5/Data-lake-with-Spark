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

![Image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/ERD.PNG)

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

### Locally in Ubuntu (Only works if you have AWS S3 and EMR access)

Ensure you have Ubuntu 16.04 or 18.04 installed.

* Download `data` from this repo and upload the folders `song_data` and `log_data` with all of its contents in your S3 bucket.
* Change the `input_data` and `output_data` paths in `etl.py` with your S3 bucket file path.
* Update the AWS Configuration details in `dl.cfg` file with yours.
* Sign into your AWS Console and under services click on **EC2** as shown in the below image
![image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/EC2.PNG)

* Click on **Key Pairs** on the left as shown in the below image.
![image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/ec2_key_pair.PNG)

* Click on **Create Key Pair** and give it a name (in my case name is spark-cluster). This will automatically download a `.pem` file in your machine. 
* Under services, Click on **EMR** as shown in the below image.
![image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/EMR.PNG)
* Click on **Create Cluster**.
![image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/create_cluster.PNG)
* Setup the configurations of your cluster to match the configurations shown in the below images and then click **Create Cluster**.
![image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/cluster_1.PNG)
![image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/cluster_2.PNG)

* You will now be redirected to a page similar to one shown in below image where it says Cluster is *Starting*. Wait until it changes to
Cluster is *Running*. This could take upto 10 minutes depending on how fast all the `m5.xlarge` instances can be provisioned.
![image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/cluster_starting.PNG)

* Once it says Cluster is *Running* like in the image below, then open a terminal in your Ubuntu machine and clone the contents of this repo into a local directory.
![image](https://github.com/arahman5/Data-lake-with-Spark/blob/master/resource/cluster_running.PNG)

* Connect to the master node of your Hadoop EMR Cluster over ssh from ubuntu terminal by following the section **"Connect to the Master Node Using SSH and an Amazon EC2 Private Key on Linux, Unix, and Mac OS X"** in this [link](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html).
* From another terminal, copy over the files `dl.cfg` and `etl.py` from your local directory to the Master node by executing the below command:
```bash
scp /pathtothefiles clustername:~/
```

In my case this for example would be:
```bash
scp ~/Desktop/sparkify_log_small.json spark-cluster:~/
```

* Make the `etl.py` script executable by executing the below command:
```bash
chmod +x etl.py
```

* Execute the below command to find out where in the master node is spark-submit located as this path may vary amongst different nodes.
```bash
which spark-submit
```

* In my case the returned path is `/usr/bin/spark-submit`. Lastly, we need to submit a spark job through the terminal by executing the below command:
```bash
/pathtosparksubmit --master yarn ./etl.py
```
In my case this for example will look like below:
```bash
/usr/bin/spark-submit --master yarn ./etl.py
```

* The script will take few minutes to finish running and you should see `.parquet` files being generated in your provided S3 bucket 
in the parameter `output_data`.




