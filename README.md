# big-data-gcw

Big Data &amp; Analytics Group Coursework

## Playing \w Ambari

1. Download the [dataset](https://www.kaggle.com/eliasdabbas/web-server-access-logs?select=access.log)
2. Normalize the dataset using `Normalize.py`
3. Load it to ambari/hive-view and play with it.
4. Or just play with spark. `RDDQueries.py` has an example.

## Normalize the dataset

This is essential for normalizing the dataset to process the business logic of our application. With the
raw `access.log` file we have overall 2% unstructured data so cleaning this early is the way to go. (Our target is to
demo this as a structured data oriented solution.)

1. Install python `3.5` or above
2. Execute it! `python3 Normalize.py /Users/yasin/Downloads/access.log 20000`
   > For testing locally: -
   > Create a smaller dataset `python3 Normalize.py <path> <batch_size> <dry_run_limit>`
3. All the cleaned data will be in `$HOME/bda-cw-workdir`

# CW Implementation

Ambari is garbage. We will use Spark how it intended used in real world production environments. It's best if you can
use a `linux` or a `macOS` machine.

### Step 0 - Clone this repo.

- Don't just download. Use `git clone` with `ssh` or `https`.

### Step 1 - Java

- Install Java `>=8` or `<=12`
- Add `$JAVA_HOME` to your shell env.

### Step 2 - Python

- Install python `>=3.5` (I use `3.9.6`)
- Then add `export PYSPARK_PYTHON="python3"` to your shell env.

### Step 3 - Setup Spark 🚀

- Download [Spark latest version](https://spark.apache.org/downloads.html)
- Move the downloaded spark `.tgz` or `.tar` anywhere in your machine.
- Extract it `tar -xvzf /path/to/spark-X.X.X-bin-hadoopX.X.tgz`
- Add `$SPARK_HOME` to your shell env.
- Append `$SPARK_HOME/bin` to your `$PATH`
- Add below configuration to: `$SPARK_HOME/conf/spark-defaults.conf`

```properties
spark.jars.packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
spark.driver.extraJavaOptions -Dlog4j.configuration=file:configs/log4j.properties
```

### Step 4 - Setup Kafka

- [Download](https://kafka.apache.org/downloads) the distribution.
- Add `$KAFKA_HOME` to your shell env.
- Append `$KAFKA_HOME/bin` to your `$PATH` (If windows then: `bin/windows`)

### Step 5 - IDE Setup

I'm using `PyCharm` with a python _virtual environment_. Install the `requirements.txt` with `pip`
on to the `venv` go get started.

1. Install Packages: `pipenv install -r requirements.txt`

### Step 6 - Bootstrapping

```bash

# Execute this in shell.
source scripts/activate.zsh && source venv/bin/activate

# Then, to start Zookeeper & Kafka
start_zookeeper_and_kafka

# Create data ingest topic. This is where servers publish
# data to ingest for Spark.
create_kafka_topic "access-logs"

# Create data sink topic. This is where will send transformed
# and completed data.
create_kafka_topic "access-logs-sink"

# Spin up the producers:-
# Note if you're not running this inside a venv then activate it!
# `source venv/bin/activate`
#
# Alternatively you could use global pip packages as well.
#
# Stopping is easy as CTRL+C
#
# This aims to simulate servers generating streams of access logs.
# You can execute this command to spawn multiple servers.
python3 Producer.py ./resources/access-logs.data  

# Create a Consumer (For debugging events) execute below sequentially or
# just use the convenient method `start_consuming`
listen_to_a_topic "access-logs"
listen_to_a_topic "access-logs-sink"

# To stop and reset everything. Note that you have to manually
# stop producers and consumers. 
clean_environment
```

### Step 7 - Running Queries

#### Run the `RDDQueries` to see file mode queries.

```bash
spark-submit --deploy-mode client RDDQueries.py
```

#### Run the `AccessLogsAnalytics` to streaming mode.

```bash
spark-submit --deploy-mode client AccessLogsAnalytics.py
```

# Appendix

### Useful Commands

> Dump all the dependencies installed `pip freeze > requirements.txt`

### References

1. [How to move large files via terminal](https://www.cloudera.com/tutorials/manage-files-on-hdfs-via-cli-ambari-files-view/1.html)
2. [How to reset Ambari Admin password](https://community.cloudera.com/t5/Community-Articles/Ambari-2-7-0-How-to-Reset-Ambari-Admin-Password-from/ta-p/248891)
3. [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
