# Pintrest Data Pipeline

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [License](#license)

## Description

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, I created a similar system using the AWS Cloud.

The project was planned for a month. It consisted of multiple steps, which I tried to briefly list below to paint the overview of the steps that were required to build a working data pipeline.

The whole process I have split into three parts:
- set-up,
- batch processing,
- streaming processing.

##  Project Dependencies

To execute this project, you must have the following modules installed:

- python-dotenv
- sqlalchemy
- requests

##  The dataset

To replicate the type of data commonly handled by Pinterest's engineers, this project includes a script, user_posting_emulation.py, which, when executed from the terminal, simulates a flow of random data points resembling those received by the Pinterest API during user data upload POST requests.

Executing the script initializes a database connector class that establishes a connection to an AWS RDS database. The database comprises three tables:

- pinterest_data: Contains information about posts being uploaded to Pinterest.
- geolocation_data: Holds data about the geolocation of each Pinterest post found in pinterest_data.
- user_data: Stores information about the user who uploaded each post present in pinterest_data.
The run_infinite_post_data_loop() method continuously cycles at random intervals between 0 and 2 seconds. During each iteration, it selects all columns of a random row from each of the three tables and compiles the data into dictionaries. These dictionaries are then printed to the console.

##  Utilized Tools

### Apache Kafka
Apache Kafka is an event streaming platform. 

### AWS MSK (Amazon Managed Streaming for Apache Kafka)
Amazon MSK is a fully managed service enabling the building and running of applications that use Apache Kafka to process streaming data. 

### AWS MSK Connect
MSK Connect is a feature of Amazon MSK that simplifies streaming data to and from Apache Kafka clusters. 

### Kafka REST Proxy
The Confluent REST Proxy provides a RESTful interface to an Apache Kafka® cluster, making it easy to produce and consume messages, view the state of the cluster, and perform administrative actions without using the native Kafka protocol or clients.

### AWS API Gateway
Amazon API Gateway is a fully managed service facilitating the creation, publication, maintenance, monitoring, and securing of APIs at any scale. APIs act as the "front door" for applications to access data, business logic, or functionality from your backend services.

### Apache Spark
Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters. 

### PySpark
PySpark is the Python API for Apache Spark. It enables real-time, large-scale data processing in a distributed environment using Python. 

### Databricks
This project uses the Databricks platform to perform Spark processing of batch and streaming data. 

## Set-up

### Setting up the EC2 instance & Apache Kafka

EC2 instances serve as the foundational components of cloud computing. Essentially, they are remote computers capable of executing code either on a cluster or a single machine. Access to an EC2 instance can be obtained through a terminal.

### AWS EC2 Instance Setup Steps:
Detailed process is described in the corresponding prerequisite lessons. Here are the general steps:
1. **Key Pair File Creation:**
   - In the AWS console, create a key-pair file for authentication.

2. **Security Group Configuration:**
   - Create a security group with the following inbound rules:
     - `HTTP: Anywhere IPv4`
     - `HTTPS: Anywhere IPv4`
     - `SSH: My IP`

3. **EC2 Instance Creation:**
   - Create an EC2 instance by selecting the Amazon Linux 2 AMI.
4. **Installation of Kafka and IAM MSK Authentication Package:**
   - After launching and connecting to the EC2 instance, install Kafka and the IAM MSK authentication package on the client EC2 machine.

5. **Configuration of Kafka Client Properties for AWS IAM Authentication:**
   - Configure the Kafka client properties to enable AWS IAM authentication.

6. **Creation of Amazon MSK Clusters:**
   - Create Amazon MSK clusters to facilitate Kafka messaging.

7. **Creation of Kafka Topics:**
   - Create three Kafka topics on the Kafka EC2 client machine for three tables. Utilize the Bootstrap server string and Apache Zookeeper connection string obtained from the MSK cluster.
### Connecting MSK Cluster to an S3 Bucket
Detailed process is described in the corresponding prerequisite lessons. Here are the general steps:
1. **Amazon S3 Bucket Creation:**
   - In the Amazon S3 console, create an S3 bucket. This bucket will serve as the destination for data extracted from Amazon RDS.

2. **IAM Role Creation:**
   - Create an IAM role with write permissions to the designated S3 bucket.

3. **VPC Endpoint Creation for S3:**
   - Create a VPC endpoint to establish a direct connection from the MSK cluster to S3, enhancing security and efficiency.

4. **MSK Connect Custom Plug-in:**
   - Create a connector using the custom plug-in, associating it with the IAM role previously created. This connector will enable the extraction and storage of data from Amazon RDS to the specified S3 bucket.

### Configuring API in AWS API Gateway
Detailed process is described in the corresponding prerequisite lessons. Here are the general steps:
1. **AWS API Gateway Setup:**
   - Create an API in AWS API Gateway using the REST API configuration.

2. **Creation of a New Child Resource:**
   - Extend the API functionality by creating a new child resource for this API. Choose the proxy resource option for the newly created child resource, setting the resource path to /{proxy+}. 

3. **Configuration of ANY Method:**
   - Establish flexibility in handling various HTTP methods by creating an ANY method for the chosen proxy resource.

### Setting up Kafka REST Proxy on the EC2 Client
Detailed process is described in the corresponding prerequisite lessons. Here are the general steps:
1. **Confluent Package Installation:**
   - Install the Confluent package for the Kafka REST proxy on the EC2 client.

2. **Modification of kafka-rest.properties:**
   - Modify the kafka-rest.properties file, specifying the bootstrap server, IAM role, and Zookeeper connection string.

3. **API Configuration in AWS API Gateway:**
   - In the previously created API in AWS API Gateway:
     - Choose HTTP Proxy as the Integration type.
     - Set the Endpoint URL to your Kafka Client Amazon EC2 Instance PublicDNS.

4. **API Deployment:**
   - Deploy the API to obtain the invoke URL.

5. **Data Emulation Python Script:**
   - Utilize a Python script for data emulation, sending data to the Kafka topics using the API Invoke URL.

6. **Data Storage in S3 Bucket:**
   - Finally, store the data into the previously created S3 Bucket in JSON format using the Kafka REST proxy and the API Invoke URL.

##  Batch processing
### Data Cleaning
To perform batch processing of data on Databricks, it is essential to establish the mounting of the S3 bucket within the platform. The notebook named mount_s3_todb.ipynb was executed on the Databricks platform, encompassing the following steps:

1. **AWS Access Key and Secret Access Key Setup:**
   - Create AWS Access Key and Secret Access Key for Databricks in AWS.
   - Upload the credential CSV file to Databricks.

2. **S3 Bucket Mounting to Databricks:**
   - Mount the S3 bucket to Databricks, load the data stored in the bucket, and create DataFrames for the three tables.

3. **Data Cleaning with PySpark:**
   - The code in batch_data_cleaning.ipynb consists of the steps:
     - Removing duplicate rows.
     - Renaming and re-ordering columns when necessary.
     - Removing null values.
     - Replacing values wherever necessary.
     - Converting datatypes as needed.
4. **Automation with AWS MWAA (Managed Workflows for Apache Airflow):**
   - Create an API token in Databricks.
   - Initialize a connection between MWAA and Databricks.

5. **DAG (Directed Acyclic Graph) Creation:**
   - Develop a DAG file in Python specifying:
     - Notebook path to be used from Databricks.
     - Cluster ID in AWS.

6. **Customization for Scheduled Runs:**
   - Customize the DAG to run at regular intervals based on the rate of incoming data.
   - At the specified intervals, the notebook specified in the DAG will run automatically, executing the defined queries.
### Data Analysis
The code in the file batch_querries.ipynb provides the results for the eight tasks listed:
1. The most popular Pinterest category, people post to, based on their country.

![Alt text](image-4.png)

2. Number of posts each category had between 2018 and 2022 and most popular category in each year.

![Alt text](image-2.png)

3. The user with the most followers for each country

![Alt text](image-5.png)

- The country with the user with most followers.

![Alt text](image-6.png)

4. The most popular category people post to, based on the age groups - 18-24, 25-35, 36-50, +50

![Alt text](image-7.png)

5. The median follower count for users in the age groups, 18-24, 25-35, 36-50, +50

![Alt text](image-8.png)

6. How many users have joined between 2015 and 2020.

![Alt text](image-10.png)

7. The median follower count of users who have joined between 2015 and 2020.

![Alt text](image-11.png)

8. The median follower count of users who have joined between 2015 and 2020, based on age group that they are part of.

![Alt text](image-12.png)

## File Structure

Add relevant file structure details here.
##  License

Include license information here.




