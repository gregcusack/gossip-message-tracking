

class ReportMetrics:
    def __init__(self):
        raise NotImplementedError("ReportMetrics class cannot be instantiated")

    @staticmethod
    def identify_non_reporting_staked_hosts(staked_host_ids, reported_host_ids):
        non_reporting_host_ids = set()
        reporting_staked_hosts = set()

        for host_id in staked_host_ids:
            if host_id not in reported_host_ids:
                non_reporting_host_ids.add(host_id[:8])
            else:
                reporting_staked_hosts.add(host_id[:8])

        print(f"len all staked host ids: {len(staked_host_ids)}")
        print(f"len all reported_host_ids: {len(reported_host_ids)}")
        print(f"# non reporting staked hosts: {len(non_reporting_host_ids)}")
        print(f"# reporting staked hosts: {len(reporting_staked_hosts)}")

        return non_reporting_host_ids

