# Databricks Command API  (doc: https://docs.databricks.com/dev-tools/api/1.2/index.html#command-execution)
# Step 1. Create an execution context
# Step 2. Submit query using the ContextID
# Step 3. Check command execution status

import requests
import time
import json
from datetime import datetime
import argparse 
from colorama import init, Fore, Back, Style

init()

parser = argparse.ArgumentParser(description='How-to use arg parser')
parser.add_argument('-i', '--cluster_id', type=str, help='Enter cluster name')
parser.add_argument('-l', '--language', type=str, default='python', help='Enter language name')
parser.add_argument('-c', '--command', type=str, help='Enter query')
args = parser.parse_args()    

# Databricks instance settings
DATABRICKS_INSTANCE = '{YOUR_DATABASE_INSTANCE_URL}'
PERSONAL_TOKEN = '{PERSONAL_ACCESS_TOKEN}'

def call_api(endpoint, params, payload, method, content_type='application/json'):
    headers = {'Authorization': 'Bearer {}'.format(PERSONAL_TOKEN), 'Content-Type':content_type}
    try:
        if method == 'GET':
            r = requests.get(url=endpoint, headers=headers, params=params)
        else:
            r= requests.post(url=endpoint, headers=headers, data=json.dumps(payload))      
        res = r.json()
        return (r.status_code, res)
    except Exception as e:
        return (r.status_code, e)

# Pre-step - List all existing clusters
if args.cluster_id is None:
    endpoint = 'https://{}/api/2.0/clusters/list'.format(DATABRICKS_INSTANCE)
    status_code, res = call_api(endpoint,None,None,'GET')
    
    for i in enumerate(res['clusters']):
        if i[1]['cluster_source'] != 'JOB':
            i[1]['worker_nodes'] = i[1]['num_workers'] if 'autoscale' not in i[1] else i[1]['autoscale']
            print(Fore.CYAN + '[Cluster No.{}]'.format(i[0]))
            print(Fore.WHITE + 'ClusterType: {}\nClusterID: {}\nClusterName: {}\nSparkVersion: {}\nDriverNode: {}\nWorkerNode: {}\nNum of Nodes: {}\nStatus: {}\n'.format(
                i[1]['cluster_source'], i[1]['cluster_id'], i[1]['cluster_name'], i[1]['spark_version'], i[1]['driver_node_type_id'], i[1]['node_type_id'], i[1]['worker_nodes'],i[1]['state']))
else:
    # Step 1 - Create execution context 
    endpoint = 'https://{}/api/1.2/contexts/create'.format(DATABRICKS_INSTANCE)
    payload = {'language': args.language, 'clusterId': args.cluster_id}
    status_code, res = call_api(endpoint,None,payload,'POST')
    if status_code == 200:
        context_id = res['id']
        print(Fore.GREEN + 'Successfully created Execution Context: {}'.format(context_id))
    else:
        print(status_code,res)
        exit(0)

    # Step 2 - Submit query (or file) 
    endpoint = 'https://{}/api/1.2/commands/execute'.format(DATABRICKS_INSTANCE)
    payload = {
        'language': 'python', 
        'clusterId': args.cluster_id, 
        'contextId' : context_id, 
        'command': args.command
    }
    status_code, res = call_api(endpoint,None,payload,'POST')
    if status_code == 200:
        command_id = res['id']
        print(Fore.GREEN + 'Successfully submitted Command: {}'.format(command_id))  
    else:
        print(status_code,res)
        exit(0)

    # Step 3 - Check command status
    print(Style.RESET_ALL)
    print('Checking status every 5 seconds...')
    while True:
        endpoint = 'https://{}/api/1.2/commands/status'.format(DATABRICKS_INSTANCE)
        params = {
            'clusterId': args.cluster_id, 
            'contextId' : context_id, 
            'commandId': command_id
        }
        status_code, res = call_api(endpoint,params,None,'GET')
        print('{} -> {}, {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), status_code, res))
        
        if res['status'] == 'Finished':
            break
        else:
            # Check run status result every 5 sec
            time.sleep(5)
