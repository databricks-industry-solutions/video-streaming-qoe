# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/video-streaming-qoe.git. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/qos.

# COMMAND ----------

import pandas as pd
import json

# COMMAND ----------

cdn_df = pd.DataFrame(
  {'cdn': ['Akamai', 'Level 3', 'Limelight'],
   'cdn_weight': [0.34, 0.33, 0.33]})

city_df = pd.DataFrame(
  {'city': ['New York City','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Fort Worth','Columbus','Charlotte'],
   'state': ['New York','California','Illinois','Texas','Arizona','Pennsylvania','Texas','California','Texas','California','Texas','Florida','Texas','Ohio','North Carolina'],
   'state_cd': ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA', 'TX', 'FL', 'TX', 'OH', 'NC'],
   'lat': [40.7128, 34.0522, 41.8781, 29.7604, 33.4484, 39.9526, 29.4241, 32.7157, 32.7767, 37.3382, 30.2672, 30.3322, 32.7555, 39.9612, 35.2271],
   'long': [74.006, 118.2437, 87.6298, 95.3698, 112.074, 75.1652, 98.4936, 117.1611, 96.797, 121.8863, 97.7431, 81.6557, 97.3308, 82.9988, 80.8431],
   'city_weight': [0.27, 0.13, 0.09, 0.08, 0.06, 0.05, 0.05, 0.05, 0.04, 0.03, 0.03, 0.03, 0.03, 0.03, 0.03]})

device_df = pd.DataFrame(
  {'platform': ['CTV', 'CTV', 'CTV', 'CTV', 'CTV', 'CTV', 'CTV', 'CTV', 'CTV', 'Mobile', 'Mobile', 'Mobile', 'Tablet', 'Tablet', 'Tablet', 'Tablet'],
   'device_type': ['Roku','Fire TV','Apple TV','Chromecast','Samsung TV','Vizio TV','Xbox','Playstation','Other CTV','Apple iPhone','Android Phone','Other Mobile','Apple iPad','Android Tablet','Amazon Fire Tablet','Other Tablet'],
   'platform_device_weight': [0.37, 0.34, 0.12, 0.06, 0.05, 0.03, 0.01, 0.01, 0.01, 0.45, 0.53, 0.02, 0.57, 0.18, 0.23, 0.02],
   'resolution_preferred': ['1080p', '1080p', '1080p', '1080p', '1080p', '1080p', '1080p', '1080p', '1080p', '720p', '720p', '720p', '1080p', '1080p', '1080p', '1080p'],
   'resolution_low': ['720p', '720p', '720p', '720p', '720p', '720p', '720p', '720p', '720p', '480p', '480p', '480p', '720p', '720p', '720p', '720p']})

isp_df = pd.DataFrame(
  {'isp': ['Xfinity', 'Charter Spectrum', 'AT&T', 'Verizon', 'CenturyLink', 'Cox', 'Altice USA', 'Frontier', 'Mediacom', 'TDS Telecom'],
   'isp_cd': ['isp_1', 'isp_2', 'isp_3', 'isp_4', 'isp_5', 'isp_6', 'isp_7', 'isp_8', 'isp_9', 'isp_10'],
   'isp_weight': [0.28, 0.26, 0.17, 0.07, 0.06, 0.05, 0.05, 0.04, 0.01, 0.01]})

platform_df = pd.DataFrame(
  {'platform': ['CTV', 'Mobile', 'Tablet'],
   'platform_weight': [0.42, 0.38, 0.2]})

resolution_df = pd.DataFrame(
  {'aspect_ratio': ['1920x1080', '1280x720', '640x480'],
   'resolution': ['1080p', '720p', '480p'],
   'bitrate_min_kbps': [3000, 1500, 500],
   'bitrate_max_kbps': [6000, 4000, 2000]})

print('Dataframes Returned:\n cdn_df \n city_df \n device_df \n isp_df \n platform_df \n resolution_df')
