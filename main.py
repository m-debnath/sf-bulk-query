# Example
# python ./main.py Asset https://kpnb2b--devbira10.sandbox.my.salesforce.com session_id
# python ./main.py BIL_Identifier_Mapping__c https://kpnb2b--devbira10.sandbox.my.salesforce.com session_id

import sys

import constants
from SalesforceJob import SalesforceJob

assert(len(sys.argv) == 4)

QUERY_SOBJECT = sys.argv[1]
INSTANCE_URL = sys.argv[2]
AUTH_HEADER = {
    constants.AUTH_HEADER_NAME: sys.argv[3]
}

if __name__ == '__main__':
    print(constants.APP_BANNER)
    with SalesforceJob(QUERY_SOBJECT, 'CSV', True, INSTANCE_URL, AUTH_HEADER) as job:
        job.submit_query(constants.JOB_TO_QUERY[QUERY_SOBJECT])
        if job.is_complete():
            job.get_complete_batches()
            job.get_results()
            job.generate_csv()
            assert(job.records_processed == job.records_written_to_csv)
