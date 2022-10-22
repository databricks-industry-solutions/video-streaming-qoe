# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/video-streaming-qoe.git. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/qos.

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://brysmiwasb.blob.core.windows.net/demos/images/ME_solution-accelerator.png"; width="50%">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Scenario: ISP Outage
# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://db-gtm-industry-solutions.s3.amazonaws.com/data/cme/qoe/images/qoe_isp_outage.png">
# MAGIC   <a href='https://e2-demo-west.cloud.databricks.com/sql/dashboards/304349fc-63c9-4b20-bbda-b10627d87d76-video-qoe-health'>Link to Dashboard</a>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Setting up Video Quality of Experience Monitoring
# MAGIC %md
# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://db-gtm-industry-solutions.s3.amazonaws.com/data/cme/qoe/images/five-step-process-v4.png"; width="50%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Stream Data In

# COMMAND ----------

# DBTITLE 1,Structured Streaming mitigates common challenges associated with streaming
# MAGIC %md
# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://db-gtm-industry-solutions.s3.amazonaws.com/data/cme/qoe/images/common-challenges.png"; width="50%">
# MAGIC </div>

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user') # user name associated with your account
base_path = "dbfs:/home/{}/qoe".format(user)
source_data_path = "{}/data/".format(base_path)
model_path = "{}/ml/iso_forest".format(base_path)
user_name_sql_compatible = user.split("@")[0].replace(".", "_")
database_name = f"{user_name_sql_compatible}_qoe"
file = dbutils.fs.ls(source_data_path)[0].name
schema = spark.read.json(source_data_path+file).schema

# COMMAND ----------

# DBTITLE 1,Create Database
sql(f"""create database if not exists {database_name}""")
sql(f"use {database_name}")

# COMMAND ----------

# DBTITLE 1,Read Stream: Autoloader
source_data_df = (
  spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .schema(schema)
  .load(source_data_path)
)

# COMMAND ----------

# DBTITLE 1,Read Stream: Kinesis
"""
source_data_df_kinesis = (
  spark.readStream
  .format("kinesis")
  .option("streamName", "video_events_stream")
  .option("region", "us-west-2")
  .option("initialPosition", "TRIM_HORIZON")
  .load()
)
"""

# COMMAND ----------

# DBTITLE 1,Read Stream: Kafka
"""
server_ip = dbutils.secrets.get("qos", "kafka-bootstrap-servers")

source_data_df_kafka = (
  spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", server_ip)
  .option("subscribe", "video_events_topic")     
  .option("startingOffsets", "latest")  
  .load()
)
"""

# COMMAND ----------

# DBTITLE 1,Read Stream: Azure Event Hubs
"""
eventHubsConf = dbutils.secrets.get("qos", "event-hubs-conf")

source_data_df_event_hubs = (
  spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()
)
"""

# COMMAND ----------

# DBTITLE 1,Write to Bronze Table
delta_bronze_path = f"dbfs:/home/{user}/qoe/delta_bronze"
bronze_checkpoint = delta_bronze_path + "/_checkpoint"
(source_data_df.writeStream.format("delta")
  .option("checkpointLocation", bronze_checkpoint)
  .trigger(once=True)
  .start(delta_bronze_path)
  .awaitTermination())

# COMMAND ----------

# DBTITLE 1,Register Bronze Table
sql(f"""create table if not exists delta_bronze
using delta
location '{delta_bronze_path}'
""")
display(spark.table("delta_bronze"))

# COMMAND ----------

# DBTITLE 1,View Bronze Table
# MAGIC %sql select * from delta_bronze

# COMMAND ----------

# DBTITLE 1,Medallion Architecture
# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://db-gtm-industry-solutions.s3.amazonaws.com/data/cme/qoe/images/medallion.png"; width="50%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Apply In-Stream Transformations

# COMMAND ----------

# DBTITLE 1,Read Stream from Bronze Table
bronze_df = spark.readStream.table("delta_bronze")

# COMMAND ----------

# DBTITLE 1,Explode Struct Columns
from pyspark.sql.functions import col

bronze_df_exploded = (
  bronze_df.select("*", col("rebuffer_stats.*"))
  .withColumnRenamed("count", "rebuffer_count")
  .withColumnRenamed("ratio", "rebuffer_ratio")
  .withColumnRenamed("seconds", "rebuffer_seconds")
  .withColumnRenamed("severity", "rebuffer_severity")
  .drop("rebuffer_stats")
)

# COMMAND ----------

# DBTITLE 1,Write to Silver Table 
delta_silver_path = f"dbfs:/home/{user}/qoe/delta_silver"
silver_checkpoint = delta_silver_path + "/_checkpoint"

(bronze_df_exploded.writeStream
  .option("checkpointLocation", silver_checkpoint)
  .format("delta")
  .trigger(once=True)
  .start(delta_silver_path)
  .awaitTermination())

# COMMAND ----------

# DBTITLE 1,Register Silver Table
sql("""create table if not exists delta_silver
using delta
location '{}'
""".format(delta_silver_path))
display(spark.table("delta_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Perform Inference to Detect Anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://db-gtm-industry-solutions.s3.amazonaws.com/data/cme/qoe/images/ml-lifecycle.png"; width="70%">
# MAGIC </div>

# COMMAND ----------

import numpy as np
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
from pyspark.sql.functions import struct, count, col
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from sklearn.ensemble import IsolationForest
import shutil

# COMMAND ----------

# DBTITLE 1,Prepare Data for Model Training
df = spark.table('delta_silver').toPandas()

# COMMAND ----------

metrics_df = df[['scenario','video_start_failure','playback_failure','exit_before_video_start',
                 'time_to_first_frame','downshift_cnt','rebuffer_ratio','rebuffer_count']].fillna(0)
base_case_df = metrics_df[metrics_df['scenario'] == 'base_case'].drop(['scenario'],axis=1)

# COMMAND ----------

shutil.rmtree(model_path.replace('dbfs:','/dbfs'),ignore_errors=True)

# COMMAND ----------

# DBTITLE 1,Train Isolation Forest
experiment_name=f'/Users/{user}/qoe'
mlflow.set_experiment(experiment_name)
with mlflow.start_run():
  max_samples = 100
  random_state = np.random.RandomState(42)
  contamination = 0.01
  
  clf = IsolationForest(max_samples=max_samples, random_state=random_state,contamination=contamination)
  clf.fit(base_case_df)
  
  mlflow.sklearn.log_model(clf, "iso_forest")
  save_model_path = model_path.replace('dbfs:','/dbfs')
  mlflow.sklearn.save_model(clf, save_model_path)

# COMMAND ----------

# DBTITLE 1,Load model and create pyfunc_udf
pyfunc_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_path)

# COMMAND ----------

# DBTITLE 1,Apply pyfunc_udf for Real-time Anomaly Detection
_ = spark.table('delta_silver').na.fill(0).withColumn('predictions', pyfunc_udf(struct('video_start_failure', 'playback_failure',\
                    'exit_before_video_start', 'time_to_first_frame','downshift_cnt', 'rebuffer_ratio','rebuffer_count')))

_.createOrReplaceTempView('predictions')

# COMMAND ----------

# DBTITLE 1,Display Anomaly Detection Results
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW play_attempts AS (
# MAGIC   SELECT isp_code, state, city, count(*) AS total_play_attempts
# MAGIC   FROM predictions
# MAGIC   GROUP BY isp_code, state, city);
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW anomalies AS (
# MAGIC   SELECT isp_code, state, city, count(*) AS anomaly_cnt
# MAGIC   FROM predictions
# MAGIC   WHERE predictions = -1
# MAGIC   GROUP BY isp_code, state, city);
# MAGIC 
# MAGIC SELECT a.isp_code,a.state,a.city, total_play_attempts, anomaly_cnt, round(anomaly_cnt/total_play_attempts,2) as pct_anomaly
# MAGIC FROM play_attempts p INNER JOIN anomalies a
# MAGIC WHERE p.isp_code = a.isp_code
# MAGIC AND p.state = a.state
# MAGIC AND p.city = a.city
# MAGIC ORDER BY pct_anomaly desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Configure Alerts

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://db-gtm-industry-solutions.s3.amazonaws.com/data/cme/qoe/images/qoe_isp_outage.png">
# MAGIC </div>
# MAGIC <a href='https://e2-demo-west.cloud.databricks.com/sql/dashboards/304349fc-63c9-4b20-bbda-b10627d87d76-video-qoe-health'>Link to Dashboard</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Conduct Root Cause Analysis
# MAGIC * [Link to SQL IDE](https://e2-demo-west.cloud.databricks.com/sql/queries/0e8c1084-ee3c-4b72-b28c-32dc5ead1a27/source)
