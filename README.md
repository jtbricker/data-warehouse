# data-warehouse

A simple example datawarehouse ETL project using python and AWS Redshift

## Purpose
The purpose of this project is to create a pipeline that is able to 
extract data from log and json files, transform the data to match required formats,
and load the transformed data into postgres database tables on AWS redshift to allow for efficient data analysis.

## Database Schema
The database tables are laid our in a star schema, with central songplays fact table:

### songplays
`songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`

and 4 supporting dimension tables, users, songs, artists, and time:

### users
`user_id, first_name, last_name, gender, level`

### songs
`song_id, title, artist_id, year, duration`

### artists
`artist_id, name, location, lattitude, longitude`

### time
`start_time, hour, day, week, month, year, weekday`

The star schema design allows for efficient querying of the songplay data across many different dimensons
(see [Example Queries])

## ETL Pipeline
The ETL pipeline works in 2 steps:

1. Copy the data from AWS S3 buckets into staging tables.
1. Modify and insert the data into respective analytics tables.


## Example Queries

Question: How many users are listening to the Backstreet Boys on Thursdays?

```SQL
SELECT COUNT(*) 
FROM songplays sp
JOIN artists a on sp.artist_id = a.artist_id
JOIN time t on sp.start_time = t.start_time
WHERE a.name = 'Backstreet Boys' AND  t.weekday = 3
```

Question: Who are the top 5 users with the most listening time?

```SQL
SELECT TOP 5 u.*, SUM(s.duration) as total_time
FROM songplays sp
JOIN artists a on sp.artist_id = a.artist_id
JOIN users u on sp.user_id = u.user_id
GROUP BY u.user_id
ORDER BY total_time DESC
```

## Setup Notes

### Prerequisites

Running this project requires:

* python 3

### Install Dependencies

Run the following to install the required dependencies (listed in `requirements.txt`)

``` bash
pip install -r requirements.txt
```

### Config File

In order to authorize resources to be created on your AWS account, the following values are required in the `dwh.cfg` (remember: DO NOT share these values with anyone or push to code repos )

* `[CLUSTER][DB_PASSWORD]` - Choose your own password
* `[AWS][KEY]`
* `[AWS][SECRET]`

See [this post](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for information on obtaining these values.

### Create Redshift Cluster

Run the following to create your redshift cluster instance

``` bash
python create_cluster.py
```

Populate the following values in the `dwh.cfg` file:

* `[CLUSTER][HOST]` - Endpoint url of your cluster
* `[IAM_ROLE][ARN]` - Amazon resource name of your IAM role

It will take a few minutes for the cluster to become available. You can check the status of your cluster on the AWS Console.

### Create Tables

Run the following to drop/create the tables on your newly created cluster

``` bash
python create_tables.py
```

### Load Data

Run the following to copy data from the S3 buckets specified in `dwh.cfg` into your staging tables, and then insert data into your analytics tables.

``` bash
python etl.py
```

This may take a while.  After this step you should see data in your 5 analytics tables.