# Databricks Get Results Locally

This utility lets you submit your own PySpark, Scala or R code to Databricks cluster and get execution results locally. Under the hood it uses Command Execution API to submit your query and retrieve results back in JSON format.

## How-To
To submit your query run:
`python main.py -i {DATABRICKS_CLUSTER_ID} -c {YOUR_SPARK_QUERY} -l {LANGUGAGE}`

## Databricks Cluster ID
To view a list of your own cluster's cluster id, run:
`python main.py`

## Databricks URL and Access Token
Add your own Databrick's workspace URL and personal access token to the `config.py` file so that your credential is used to authenticate with the Databricks REST API endpoint


For a detailed tutorial on how to run this utility, refer to this [article](https://jixjia.com/2020/05/12/get-databricks-results-locally/) 
