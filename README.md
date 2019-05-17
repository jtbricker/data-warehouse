# data-warehouse

A simple example datawarehouse ETL project using python and AWS Redshift

## Setup Notes

### IAM Role

You need an IAM Role with read access to S3.  Copy the `ARN` into `dwh.cfg`.

### Security Group 

You need a security group that accepts TCP/IP connections on port 5439

* EC2 Dashboard -> Security Groups -> Choose/ Create Security Group -> (Right Click) Edit Inbound Rules -> Redshift (Type), 0.0.0.0/0 Custome (Source)  

### Start a Redshift Cluster

* Specify your IAM user and configured security group

* Put your `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_PORT` values in the `dwh.cfg` file.

* Once the cluster is intialized, get the `HOST` name from the instance details "endpoint" (don't include port)