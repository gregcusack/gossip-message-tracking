

class ReportMetrics:
    def __init__(self):
        raise NotImplementedError("ReportMetrics class cannot be instantiated")

    @staticmethod
    def identify_non_reporting_hosts(all_host_ids, reported_host_ids):
        # print(all_host_ids)
        non_reporting_host_ids = set()
        reporting_hosts = set()
        # print(type(all_host_ids))
        # print(type(reported_host_ids))

        for host_id in all_host_ids:
            if host_id not in reported_host_ids:
                non_reporting_host_ids.add(host_id[:8])
            else:
                reporting_hosts.add(host_id[:8])

        # print(non_reporting_hosts)
        print(f"len all_host ids: {len(all_host_ids)}")
        print(f"len reported_host_ids: {len(reported_host_ids)}")
        print(f"# non reporting hosts: {len(non_reporting_host_ids)}")
        print(f"# reporting hosts: {len(reporting_hosts)}")

        return non_reporting_host_ids

        # print(non_reporting_hosts)

