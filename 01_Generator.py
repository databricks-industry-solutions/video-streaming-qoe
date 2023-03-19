# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/video-streaming-qoe.git. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/qos.

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://brysmiwasb.blob.core.windows.net/demos/images/ME_solution-accelerator.png"; width="50%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notes 
# MAGIC * In this notebook, you will generate synthetic video quality of experience data. The default setting is 200K records.
# MAGIC * Prior to running this notebook, please make sure that the files referenced in Cmd 5 are loaded into the corresponding directory.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col
from faker import Faker
from faker.providers import internet, misc, date_time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import random
import uuid
import json
import shutil

# COMMAND ----------

fake = Faker()
fake.add_provider(internet) #not used
fake.add_provider(misc)
fake.add_provider(date_time)

# COMMAND ----------

# Define file paths
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user') # user name associated with your account
basePath = '/dbfs/home/{}/qoe/rules/'.format(user)

# COMMAND ----------

# MAGIC %run ./00_Dims

# COMMAND ----------

# Config
start_date = '2020-12-01'
end_date = '2020-12-02'
num_videos = 25

# Scenario Flags (0=off,1=on)
faulty_device = 1
corrupted_video_asset = 1
isp_outage = 1

# COMMAND ----------

# Generate Video Metadata: {video_name: content_length_minutes}
random.seed(0)
video_dict = {'video_{}'.format(i):random.choice([5,15,30]) for i in range(1,num_videos+1)}

# COMMAND ----------

########################################
# HELPER FUNCTIONS
########################################

def getRandomWeighted(df,vals,weights):
  return random.choices(list(df[vals]),list(df[weights]))[0]

########################################
# DEVICE & GEO FUNCTIONS
########################################

def getDeviceId():
  return fake.uuid4()

def getPlatform(override=False):
  return getRandomWeighted(platform_df,'platform','platform_weight')
  
def getDeviceType(platform):
  device_options_df = device_df[device_df['platform'] == platform]
  return getRandomWeighted(device_options_df,'device_type','platform_device_weight')

def getCountry():
  return 'US'

def getCity():
  return getRandomWeighted(city_df,'city','city_weight')

def getState(city):
  return city_df[city_df['city'] == city]['state'].values[0]

def getStateCode(state):
  return city_df[city_df['state'] == state]['state_cd'].values[0]

def getConnectionType():
  return random.choice(['wifi','cellular','fixed']) # NEED TO STANDARDIZE THIS

########################################
# CONTENT DELIVERY FUNCTIONS
########################################

def getISP():
  return getRandomWeighted(isp_df,'isp','isp_weight')

def getISPCode(isp):
  return isp_df[isp_df['isp'] == isp]['isp_cd'].values[0]

def getCDN():
  return getRandomWeighted(cdn_df,'cdn','cdn_weight')

def getVideoName():
  return "video_{}".format(random.choice(range(1,num_videos+1)))

def getContentLength(video_name):
  return video_dict[video_name]

def getStartTime():
  start_time_fmt = datetime.strptime(start_date, "%Y-%m-%d")
  end_time_fmt = datetime.strptime(end_date, "%Y-%m-%d")
  return fake.date_time_between_dates(start_time_fmt, end_time_fmt)

def getPercentViewed(exit_before_video_start):
  if exit_before_video_start == 1:
    return 0
  elif exit_before_video_start == 0:
    return round(random.choice(range(0,101))/100,2) # Need to add better handling for 0. Needs to be dependent on vsf = 0,1

def getViewTime(content_length,exit_before_video_start):
  pct_viewed = getPercentViewed(exit_before_video_start)
  minutes = int(int(content_length) * pct_viewed) 
  seconds = random.randint(0,59) if exit_before_video_start == 0 else random.randint(0,10)
  return timedelta(minutes=minutes,seconds=seconds)

def getVideoStartFailure(choices=[0,1],probs=[0.99,0.01]):
  return random.choices(choices,probs)[0]

def getExitBeforeVideoStart(video_start_failure,choices=[0,1],probs=[0.93,0.07]):
  if video_start_failure == 1:
    return 1
  elif video_start_failure == 0:
    return random.choices(choices,probs)[0]

def getTimeToFirstFrame(exit_before_video_start):
  if exit_before_video_start == 1:
    return None
  elif exit_before_video_start == 0:
    return round(np.abs(random.gauss(4.5,1)),2)

def getPlaybackFailure(exit_before_video_start,choices=[0,1],probs=[0.99,0.01]):
  if exit_before_video_start == 1:
    return None 
  elif exit_before_video_start == 0:
    return random.choices(choices,probs)[0]

def getRebufferSeverity(exit_before_video_start, playback_failure,override=False):
  if exit_before_video_start == 1:
    return None
  elif override == True: # for isp_outage scenario
    return 'critical'
  elif playback_failure == 1:
    return random.choices(['mild','moderate','critical'],[0.0,0.08,0.92])[0]
  else: #playback_failure = 0 & ebvs = 0
    return random.choices(['mild','moderate','critical'],[0.35,0.63,0.02])[0]
  
def getRebufferRatio(exit_before_video_start, rebuffer_severity):
  if exit_before_video_start == 1:
    return None
  elif (exit_before_video_start == 0) & (rebuffer_severity == 'mild'):
    return round(random.uniform(0.0,0.005),3)
  elif (exit_before_video_start == 0) & (rebuffer_severity == 'moderate'):
    return round(random.uniform(0.005,0.020),3)
  elif (exit_before_video_start == 0) & (rebuffer_severity == 'critical'):
    return round(random.uniform(0.020,0.040),3)

def getRebufferSeconds(exit_before_video_start, content_length_minutes,rebuffer_ratio):
  if exit_before_video_start == 1:
    return None
  else:
    content_length_seconds = (content_length_minutes * 60)
    return round(content_length_seconds * rebuffer_ratio,2)

def getRebufferCount(exit_before_video_start, rebuffer_severity):
# May need to revist allocations
  if exit_before_video_start == 1:
    return None
  elif (exit_before_video_start == 0) & (rebuffer_severity == 'mild'):
    return random.choices([0,1],[0.7,0.3])[0]
  elif (exit_before_video_start == 0) & (rebuffer_severity == 'moderate'):
    return random.choices([1,2],[0.8,0.2])[0]
  elif (exit_before_video_start == 0) & (rebuffer_severity == 'critical'):
    return random.choices([2,3],[0.7,0.3])[0]

def getResolutionType(exit_before_video_start,content_length_minutes, rebuffer_count):
  if exit_before_video_start == 1:
    return 'preferred'
  elif (exit_before_video_start == 0) & (content_length_minutes == 5) & (rebuffer_count >= 1):
    return 'low'
  elif (exit_before_video_start == 0) & (content_length_minutes == 15) & (rebuffer_count >= 2):
    return 'low'
  elif (exit_before_video_start == 0) & (content_length_minutes == 30) & (rebuffer_count >= 2):
    return 'low'
  else:
    return 'preferred'
    
def getResolution(device_type, resolution_type):
  return device_df[device_df['device_type'] == device_type]['resolution_{}'.format(resolution_type)].item()

def getAspectRatio(resolution):
  return resolution_df[resolution_df['resolution'] == resolution]['aspect_ratio'].item()

def getAverageBitrateKbps(exit_before_video_start, resolution,resolution_type):
  bitrate_min = resolution_df[resolution_df['resolution'] == resolution]['bitrate_min_kbps'].item()
  bitrate_max = resolution_df[resolution_df['resolution'] == resolution]['bitrate_max_kbps'].item()
  bitrate_diff = (bitrate_max - bitrate_min)
  
  if exit_before_video_start == 1:
    return None
  elif (exit_before_video_start == 0) & (resolution_type == 'preferred'):
    return random.randint(bitrate_min,bitrate_max)
  elif (exit_before_video_start == 0) & (resolution_type == 'low'):
    return random.randint(bitrate_min, (bitrate_min + (bitrate_diff/2) + 249)) # this equates to the min for next highest resolution

def getAverageBandwidthKbps(exit_before_video_start,average_bitrate_kbps,resolution_type):
  if exit_before_video_start == 1:
    return None
  elif (exit_before_video_start == 0) & (resolution_type == 'preferred'):
    bandwidth_multiplier = round(random.uniform(1.35,1.65),2) # bandwidth should be around 1.5 x bitrate
    return int(average_bitrate_kbps * bandwidth_multiplier)
  elif (exit_before_video_start == 0) & (resolution_type == 'low'):
    bandwidth_multiplier = round(random.uniform(1.45,1.75),2) # bandwidth should be higher if low bitrate
    return int(average_bitrate_kbps * bandwidth_multiplier)

def getUpshiftCnt(exit_before_video_start, resolution_type, platform):
  # Mostly noise
  if exit_before_video_start == 1:
    return None
  elif (exit_before_video_start == 0) & (resolution_type == 'preferred') & (platform == 'Tablet'):
    return random.choices([0,1],[0.8,0.2])[0]
  elif (exit_before_video_start == 0) & (resolution_type =='preferred') & (platform != 'Tablet'):
    return random.choices([0,1],[0.95,0.05])[0]
  else:
    return 0

def getDownshiftCnt(exit_before_video_start, resolution_type, override=False):
  if exit_before_video_start == 1:
    return None
  elif override == True: # for isp_outage
    return random.choices([0,1],[0.1,0.9])[0]
  elif (exit_before_video_start == 0) & (resolution_type == 'preferred'):
    return 0
  elif (exit_before_video_start == 0) & (resolution_type == 'low'):
    return random.choices([0,1],[0.8,0.2])[0]
  else:
    return 0

def getLowQualityExperience(video_start_failure, playback_failure, rebuffer_severity):
  if (video_start_failure == 1) | (playback_failure == 1) | (rebuffer_severity == 1):
    return 1
  else:
    return 0

########################################
# CONSTRUCT EVENT
########################################

def getEvent():
  device_id = getDeviceId()
  platform = getPlatform()
  device_type = getDeviceType(platform)
  country = getCountry()
  city = getCity()
  state = getState(city)
  state_code = getStateCode(state)
  connection_type = getConnectionType()
  isp = getISP()
  isp_code = getISPCode(isp)
  cdn = getCDN()
  video_name = getVideoName()
  content_length_minutes = getContentLength(video_name)
  start_time = getStartTime()
  video_start_failure = getVideoStartFailure()
  exit_before_video_start = getExitBeforeVideoStart(video_start_failure)
  view_time = getViewTime(content_length_minutes,exit_before_video_start)
  end_time = (start_time + view_time).strftime('%Y-%m-%d %H:%M:%S')
  view_time_seconds = view_time.seconds
  time_to_first_frame = getTimeToFirstFrame(exit_before_video_start)
  playback_failure = getPlaybackFailure(exit_before_video_start)
  rebuffer_severity = getRebufferSeverity(exit_before_video_start, playback_failure)
  rebuffer_ratio = getRebufferRatio(exit_before_video_start, rebuffer_severity)
  rebuffer_seconds = getRebufferSeconds(exit_before_video_start, content_length_minutes,rebuffer_ratio)
  rebuffer_count = getRebufferCount(exit_before_video_start, rebuffer_severity) 
  resolution_type = getResolutionType(exit_before_video_start,content_length_minutes,rebuffer_count)
  resolution = getResolution(device_type, resolution_type)
  aspect_ratio = getAspectRatio(resolution)
  average_bitrate_kbps = getAverageBitrateKbps(exit_before_video_start, resolution,resolution_type)
  avg_bandwidth_kbps = getAverageBandwidthKbps(exit_before_video_start,average_bitrate_kbps, resolution_type)
  upshift_cnt = getUpshiftCnt(exit_before_video_start, resolution_type,platform)
  downshift_cnt = getDownshiftCnt(exit_before_video_start, resolution_type)
  low_quality_experience = getLowQualityExperience(video_start_failure, playback_failure, rebuffer_severity)
  play_attempt = 1
  
##############################################
# IDENTIFY SCENARIO (BASE CASE + EDGE CASES)
##############################################
 
  if (device_type == 'Amazon Fire Tablet') & (faulty_device == 1):
    scenario = 'faulty_device'
  elif (video_name == 'video_23') & (resolution == '1080p') & (corrupted_video_asset == 1):
    scenario = 'corrupted_video_asset'
  elif (isp == 'Charter Spectrum') & (city == 'Los Angeles') & (isp_outage == 1):
    scenario = 'isp_outage'
  else:
    scenario = 'base_case'

##############################################
# OVERRIDE DATA FOR EDGE CASES
##############################################

  if scenario == 'faulty_device':
    #higher probs
    video_start_failure = getVideoStartFailure(probs=[0.35,0.65])
    
    #standard calls
    exit_before_video_start = getExitBeforeVideoStart(video_start_failure)
    view_time = getViewTime(content_length_minutes,exit_before_video_start)
    end_time = (start_time + view_time).strftime('%Y-%m-%d %H:%M:%S')
    view_time_seconds = view_time.seconds
    time_to_first_frame = getTimeToFirstFrame(exit_before_video_start)
    
    #higher probs
    playback_failure = getPlaybackFailure(exit_before_video_start,probs=[0.2,0.8])
    
    #standard calls
    rebuffer_severity = getRebufferSeverity(exit_before_video_start, playback_failure)
    rebuffer_ratio = getRebufferRatio(exit_before_video_start, rebuffer_severity)
    rebuffer_seconds = getRebufferSeconds(exit_before_video_start, content_length_minutes,rebuffer_ratio)
    rebuffer_count = getRebufferCount(exit_before_video_start, rebuffer_severity) 
    resolution_type = getResolutionType(exit_before_video_start,content_length_minutes,rebuffer_count)
    resolution = getResolution(device_type, resolution_type)
    aspect_ratio = getAspectRatio(resolution)
    average_bitrate_kbps = getAverageBitrateKbps(exit_before_video_start, resolution,resolution_type)
    avg_bandwidth_kbps = getAverageBandwidthKbps(exit_before_video_start,average_bitrate_kbps, resolution_type)
    upshift_cnt = getUpshiftCnt(exit_before_video_start, resolution_type,platform)
    downshift_cnt = getDownshiftCnt(exit_before_video_start, resolution_type)
    low_quality_experience = getLowQualityExperience(video_start_failure, playback_failure, rebuffer_severity)
  
  if scenario == 'corrupted_video_asset':
    #higher probs
    video_start_failure = getVideoStartFailure(probs=[0.00,1.00])
    
    #standard calls
    exit_before_video_start = getExitBeforeVideoStart(video_start_failure)
    view_time = getViewTime(content_length_minutes,exit_before_video_start)
    end_time = (start_time + view_time).strftime('%Y-%m-%d %H:%M:%S')
    view_time_seconds = view_time.seconds
    time_to_first_frame = getTimeToFirstFrame(exit_before_video_start)
    playback_failure = getPlaybackFailure(exit_before_video_start)
    rebuffer_severity = getRebufferSeverity(exit_before_video_start, playback_failure)
    rebuffer_ratio = getRebufferRatio(exit_before_video_start, rebuffer_severity)
    rebuffer_seconds = getRebufferSeconds(exit_before_video_start, content_length_minutes,rebuffer_ratio)
    rebuffer_count = getRebufferCount(exit_before_video_start, rebuffer_severity) 
    resolution_type = getResolutionType(exit_before_video_start,content_length_minutes,rebuffer_count)
    resolution = getResolution(device_type, resolution_type)
    aspect_ratio = getAspectRatio(resolution)
    average_bitrate_kbps = getAverageBitrateKbps(exit_before_video_start, resolution,resolution_type)
    avg_bandwidth_kbps = getAverageBandwidthKbps(exit_before_video_start,average_bitrate_kbps, resolution_type)
    upshift_cnt = getUpshiftCnt(exit_before_video_start, resolution_type,platform)
    downshift_cnt = getDownshiftCnt(exit_before_video_start, resolution_type)
    low_quality_experience = getLowQualityExperience(video_start_failure, playback_failure, rebuffer_severity)
  
  if scenario == 'isp_outage':
    #higher probs
    video_start_failure = getVideoStartFailure(probs=[0.5,0.5])
    
    #standard calls
    exit_before_video_start = getExitBeforeVideoStart(video_start_failure)       
    view_time = getViewTime(content_length_minutes,exit_before_video_start)
    end_time = (start_time + view_time).strftime('%Y-%m-%d %H:%M:%S')
    view_time_seconds = view_time.seconds
    time_to_first_frame = getTimeToFirstFrame(exit_before_video_start)
    
    #higher probs
    playback_failure = getPlaybackFailure(exit_before_video_start,probs=[0.2,0.8])
    
    #set override to True
    rebuffer_severity = getRebufferSeverity(exit_before_video_start, playback_failure,override=True)
    
    #standard calls
    rebuffer_ratio = getRebufferRatio(exit_before_video_start, rebuffer_severity)
    rebuffer_seconds = getRebufferSeconds(exit_before_video_start, content_length_minutes,rebuffer_ratio)
    rebuffer_count = getRebufferCount(exit_before_video_start, rebuffer_severity) 
    resolution_type = getResolutionType(exit_before_video_start,content_length_minutes,rebuffer_count)
    resolution = getResolution(device_type, resolution_type)
    aspect_ratio = getAspectRatio(resolution)
    average_bitrate_kbps = getAverageBitrateKbps(exit_before_video_start, resolution,resolution_type)
    avg_bandwidth_kbps = getAverageBandwidthKbps(exit_before_video_start,average_bitrate_kbps, resolution_type)
    upshift_cnt = getUpshiftCnt(exit_before_video_start, resolution_type,platform)
    
    #set override to True
    downshift_cnt = getDownshiftCnt(exit_before_video_start, resolution_type,override=True)
    
    #standard calls
    low_quality_experience = getLowQualityExperience(video_start_failure, playback_failure, rebuffer_severity)

##############################################
# CONSTRUCT AND RETURN EVENT
##############################################

  event = {
    'device_id': device_id,
    'platform': platform,
    'device_type': device_type,
    'country': country,
    'city': city,
    'state': state,
    'state_code': state_code,
    'connectionType': connection_type,
    'isp': isp,
    'isp_code': isp_code,
    'cdn': cdn,
    'video_name': video_name,
    'content_length_minutes': content_length_minutes,
    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
    'end_time': end_time,
    'view_time_seconds': view_time.seconds,
    'video_start_failure': video_start_failure,
    'exit_before_video_start': exit_before_video_start,
    'time_to_first_frame': time_to_first_frame,
    'playback_failure': playback_failure,
    'rebuffer_stats': {
      'severity':rebuffer_severity,
      'ratio': rebuffer_ratio,
      'seconds': rebuffer_seconds,
      'count': rebuffer_count},
    'resolution': resolution,
    'aspect_ratio': aspect_ratio,  
    'average_bitrate_kbps': average_bitrate_kbps,
    'avg_bandwidth_kbps': avg_bandwidth_kbps,
    'upshift_cnt': upshift_cnt,
    'downshift_cnt': downshift_cnt,
    'play_attempt': play_attempt,
    'low_quality_experience': low_quality_experience,
    'scenario':scenario
  }

  return(event)


  
getEvent()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Generate data

# COMMAND ----------

dbutils.fs.rm('/home/{}/qoe/'.format(user), True) # delete existing files, including clearing the bronze and silver tables
dbutils.fs.mkdirs('/home/{}/qoe/data/'.format(user)) # recreate subdirectory

# COMMAND ----------

def generateData(num_records,records_per_file):
  num_of_files = round(num_records / records_per_file)
  for file in range(num_of_files): 
    file_name = uuid.uuid4()
    
    lines = ['\n'+json.dumps(getEvent()) for i in range(records_per_file)] 
    
    with open("/dbfs/home/{}/qoe/data/{}".format(user,file_name), 'w') as f:
      f.writelines(lines)

# COMMAND ----------

generateData(200000,100) # num_of_records, records_per_file

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Faker|MIT License|https://github.com/joke2k/faker/blob/master/LICENSE.txt|https://github.com/joke2k/faker|
# MAGIC |MLflow|Apache-2.0 License |https://github.com/mlflow/mlflow/blob/master/LICENSE.txt|https://github.com/mlflow/mlflow|
# MAGIC |Numpy|BSD-3-Clause License|https://github.com/numpy/numpy/blob/master/LICENSE.txt|https://github.com/numpy/numpy|
# MAGIC |Pandas|BSD 3-Clause License|https://github.com/pandas-dev/pandas/blob/master/LICENSE|https://github.com/pandas-dev/pandas|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Scikit learn|BSD 3-Clause License|https://github.com/scikit-learn/scikit-learn/blob/main/COPYING/|https://github.com/scikit-learn/scikit-learn|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|
