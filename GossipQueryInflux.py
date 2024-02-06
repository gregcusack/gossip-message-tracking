from influxdb import InfluxDBClient
from datetime import datetime
import sys
from dotenv import load_dotenv
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from GossipCrdsSample import GossipCrdsSample


class GossipQueryInflux():
    def __init__(self):
        load_dotenv()

        self.database = os.getenv("db")
        username = os.getenv("u")
        password = os.getenv("p")
        host = os.getenv("host")
        port = os.getenv("port")

        self.client = InfluxDBClient(database=self.database, username=username, password=password, host=host, ssl=True, verify_ssl=True, port=port, timeout=300, retries=0)

    def execute_query(self, query):
        return self.client.query(query)#, chunked=True, chunk_size=5000)

    def general_query(self):
        query = 'select "from", "signature", "origin", "host_id" FROM "' + self.database + '"."autogen"."gossip_crds_sample" WHERE time > now() - 14d'
        return self.execute_query(query)

    def query_last_day(self):
        query = 'select \
            "from", \
            "signature", \
            "origin", \
            "host_id" \
            FROM "' + self.database + '"."autogen"."gossip_crds_sample" \
            WHERE time > now() - 1d'#4d and time < now() - 13d'

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
    This converts a SINGLE query result into something consumable  by the Graph
    Must call this on the query result and pass in the resulting list
    to the graph.build(data) method
    TODO: should its own struct with each of these values
    """
    def transform_query_results(self, result):
        return [
            GossipCrdsSample(
                timestamp=point['time'],
                origin=point['origin'],
                source=point['from'],  # Assuming 'from' should be mapped to 'source'
                signature=point['signature'],
                host_id=point['host_id']
            )
            for point in result.get_points()
        ]

    """
    This converts MULTIPLE query results into something consumable  by the Graph
    Must call this on the query result and pass in the resulting list
    to the graph.build(data) method
    """
    def convert_query_results_to_tuple(self, results):
        data = []
        for result in results:
            data.append(self.transform_query_results(result))

        return data


    """
    Get all data comining from a specific origin
    """
    def get_data_by_single_origin(self, origin):
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

    @staticmethod
    def chunk_host_ids(host_ids, chunk_size):
        """Yield successive chunk_size chunks from host_ids."""
        for i in range(0, len(host_ids), chunk_size):
            yield host_ids[i:i + chunk_size]

    def build_query(self, chunk, signature):
        host_id_conditions = ' or '.join([f'''("host_id"='{host_id}' or "from"='{host_id[:8]}')''' for host_id in chunk])
        return f'''select "from", "signature", "origin", "host_id"
                    FROM "{self.database}"."autogen"."gossip_crds_sample"
                    WHERE time > now() - 14d and signature='{signature}' and ({host_id_conditions})'''

    def get_data_by_signature_and_host_ids(self, signature, host_ids, chunk_size=15):
        results = []
        for chunk in GossipQueryInflux.chunk_host_ids(host_ids, chunk_size):
            res = self.execute_query(self.build_query(chunk, signature))
            results.append(res)
        return results

    def get_data_by_signature_and_host_ids_threaded(self, signature, host_ids, chunk_size=10):
        results = []
        lock = threading.Lock()
        futures = []

        with ThreadPoolExecutor() as executor:
            for chunk in GossipQueryInflux.chunk_host_ids(host_ids, chunk_size):
                # Schedule the tasks and store the future objects
                future = executor.submit(self.execute_query, self.build_query(chunk, signature))
                futures.append(future)

            # Iterate over the futures as they complete
            for future in as_completed(futures):
                res = future.result()  # This will block until the future is complete
                with lock:
                    print("results len: " + str(len(results)))
                    print(type(res))
                    results.append(res)  # Assuming res is a list of results

        return results

    """
    origins: list of origin nodes to get data from
    """
    def get_data_by_multiple_origins(self, origins):
        if len(origins) < 1:
            print("error origins < 1")
            return

        origin_conditions = ' or '.join([f''' "origin"='{origin[:8]}' ''' for origin in origins])
        query = f'''select "from", "signature", "origin", "host_id"
                    FROM "{self.database}"."autogen"."gossip_crds_sample"
                    WHERE time > now() - 14d and ({origin_conditions})'''

        return self.execute_query(query)
