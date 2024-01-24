from influxdb import InfluxDBClient
from datetime import datetime
import sys
from dotenv import load_dotenv
import os

class GossipQueryInflux():
    def __init__(self):
        load_dotenv()

        self.database = os.getenv("db")
        username = os.getenv("u")
        password = os.getenv("p")
        host = os.getenv("host")
        port = os.getenv("port")

        self.client = InfluxDBClient(database=self.database, username=username, password=password, host=host, ssl=True, verify_ssl=True, port=port)

    def execute_query(self, query):
        return self.client.query(query)

    def general_query(self):
        query = 'select "from", "signature", "origin", "host_id" FROM "' + self.database + '"."autogen"."gossip_crds_sample" WHERE time > now() - 14d'
        return self.execute_query(query)

    """
    This gets the intial set of nodes an origin sends its messages to
    """
    def get_initial_egress_messages_by_signature(self, signature):
        query = 'select \
            "from", \
            "signature", \
            "origin", \
            "host_id" \
            FROM "' + self.database + '"."autogen"."gossip_crds_sample" \
            WHERE time > now() - 14d \
            and signature=\'' + signature + '\' \
            and "from"="origin"'

        return self.execute_query(query)

    """
    This converts the query result into something consumable  by the Graph
    Must call this on the query result and pass in the resulting list
    to the graph.build(data) method
    """
    def convert_query_result_to_tuple(self, result):
        data = []
        for point in result.get_points():
            data.append((point['origin'], point['signature'], point['from'], point['host_id']))

        return data


    """
    Get all data comining from a specific origin
    """
    def get_data_by_origin(self, origin):
        query = 'select \
            "from", \
            "signature", \
            "origin", \
            "host_id" \
            FROM "' + self.database + '"."autogen"."gossip_crds_sample" \
            WHERE time > now() - 14d \
            and origin=\'' + origin + '\''
        return self.execute_query(query)

    """
    Get all message data with specific signature
    """
    def get_data_by_signature(self, signature):
        query = 'select \
            "from", \
            "signature", \
            "origin", \
            "host_id" \
            FROM "' + self.database + '"."autogen"."gossip_crds_sample" \
            WHERE time > now() - 14d \
            and signature=\'' + signature + '\''
        return self.execute_query(query)