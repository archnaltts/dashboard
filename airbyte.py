import json
import random
import configparser
from prefect import flow, task
from prefect_airbyte.server import AirbyteServer
from prefect_airbyte.connections import AirbyteConnection
from prefect_airbyte.flows import run_connection_sync
import requests

config = configparser.ConfigParser()
config.read("config.ini")

# importing all the config
all_servers = json.loads(config['server']['all_servers'])
server_port = config['server']['port']
username = config['server']['username']
password = config['server']['password']
connections_url = config['server']['url']

# Maintain a list of servers
list_server = list(all_servers.keys())

#Read the list of connections from each
@task(name='server_connection_list')
def server_connection_list(list_server,username,password,server_port):
    servers_connection_ids={}
    for server in list_server:
        workspace_id = all_servers[server]
        json_data = {"workspaceId": workspace_id}
        connection_url = connections_url.format(server, server_port)
        response = requests.post(url=connection_url, json=json_data, auth=(username, password))
        all_connection_ids = [i['connectionId'] for i in response.json()['connections'] if response.json()['connections']]
        servers_connection_ids[server]=all_connection_ids
    return servers_connection_ids

@flow(name="Sync all Connection Flow")
def sync_connections():

    #calling function to get connection list from all servers.
    connection_host_ids = server_connection_list(list_server,username,password,server_port)

    #Random connection picked from retieved connections.
    server_host = random.choice(list(connection_host_ids.items()))

    #for all connection id run connection sync
    for con_id in server_host[1]:
        print("*************************** con_id ********** : ",con_id, type(server_host[0]), str(server_host[0]))
        airbyte_sync_result = run_connection_sync(
            airbyte_connection=AirbyteConnection(airbyte_server= AirbyteServer(
                                                                                server_host=str(server_host[0]),
                                                                                server_port=8000
                                                                            ),
                                                 connection_id=con_id))
        try:
            print("records_synced : ",airbyte_sync_result.records_synced)
            print("job_status : ",airbyte_sync_result.job_status)
            print("updated_at : ",airbyte_sync_result.updated_at)
        except:
            print("no update")


if __name__ == "__main__":
    sync_connections()

