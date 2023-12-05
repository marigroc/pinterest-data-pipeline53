# Pintrest Data Pipeline

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [License](#license)

## Description

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud.

## Installation

1. Set up GitHub by creating a new repository.
2. Set up AWS by creating a new AWS cloud account.
3. Download the Pinterest infrastructure using the provided script and RDS database credentials.
4. Sign in to the AWS console, change the password, and note the UserId.
5. Configure the EC2 Kafka client by creating a .pem key file, connecting to the EC2 instance, installing Kafka, and creating Kafka topics.
6. Connect the MSK cluster to the S3 bucket by creating a custom plugin with MSK Connect and a connector.
7. Configure an API in API Gateway by building a Kafka REST proxy integration method.

## Usage

- Follow the steps in each milestone to set up the environment, configure the EC2 Kafka client, connect the MSK cluster to the S3 bucket, and configure an API in API Gateway.
- Modify the user_posting_emulation.py script to send data to Kafka topics using the API Invoke URL.
- Check data consumption in Kafka topics and verify data storage in the designated S3 bucket.

## File Structure

Add relevant file structure details here.
##  License

Include license information here.




