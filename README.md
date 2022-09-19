<div style="text-align: left">
  <img src="https://brysmiwasb.blob.core.windows.net/demos/images/ME_solution-accelerator.png"; width="50%">
</div>

## Scenario: ISP Outage

<div style="text-align: left">
  <img src="https://db-gtm-industry-solutions.s3.amazonaws.com/data/cme/qoe/images/qoe_isp_outage.png">
  <a href='https://e2-demo-west.cloud.databricks.com/sql/dashboards/304349fc-63c9-4b20-bbda-b10627d87d76-video-qoe-health'>Link to Dashboard</a>
</div>

## Setting up Video Quality of Experience Monitoring

<div style="text-align: left">
  <img src="https://db-gtm-industry-solutions.s3.amazonaws.com/data/cme/qoe/images/five-step-process-v4.png"; width="50%">
</div>
___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

|Library Name|Library license | Library License URL | Library Source URL |
|---|---|---|---|
|Faker|MIT License|https://github.com/joke2k/faker/blob/master/LICENSE.txt|https://github.com/joke2k/faker|
|MLflow|Apache-2.0 License |https://github.com/mlflow/mlflow/blob/master/LICENSE.txt|https://github.com/mlflow/mlflow|
|Numpy|BSD-3-Clause License|https://github.com/numpy/numpy/blob/master/LICENSE.txt|https://github.com/numpy/numpy|
|Pandas|BSD 3-Clause License|https://github.com/pandas-dev/pandas/blob/master/LICENSE|https://github.com/pandas-dev/pandas|
|Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
|Scikit learn|BSD 3-Clause License|https://github.com/scikit-learn/scikit-learn/blob/main/COPYING/|https://github.com/scikit-learn/scikit-learn|
|Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|

To run this accelerator, clone this repo into a Databricks workspace. Attach the RUNME notebook to any cluster running a DBR 11.0 or later runtime, and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. Execute the multi-step-job to see how the pipeline runs.

The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.
