# Databricks Get Results Locally

This utility lets you submit your own PySpark, Scala or R code to Databricks cluster and get execution results back locally. Under the hood it uses Command Execution API to post your code and retrieve results back in JSON format.

## Quick Start
To submit your ad-hoc query run:
`python main.py -i {DATABRICKS_CLUSTER_ID} -c {YOUR_SPARK_QUERY} -l {LANGUGAGE}`

E.g.
<img src="https://jixjiastorage.blob.core.windows.net/blog-resources/databricks-command-api/4.%20submit_job.gif">


## Databricks Cluster ID
To view a list of your own cluster's cluster id, run:
`python main.py`

E.g.
<img src="https://jixjiastorage.blob.core.windows.net/blog-resources/databricks-command-api/3.%20list_clusters.gif">

## Databricks URL and Access Token
Add your own **Databrick's workspace URL** and **personal access token** to the `config.py` file so that your query can authenticate against the Databricks REST API endpoint

E.g.
<img src="https://jixjiastorage.blob.core.windows.net/blog-resources/databricks-command-api/6.%20config.gif">



#### For a friendly tutorial on how to run this utility refer to this [article](https://jixjia.com/2020/05/12/get-databricks-results-locally/).
