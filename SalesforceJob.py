from datetime import datetime
from functools import reduce
import requests
import time
from xml.etree import ElementTree

import constants

class SalesforceJob:

    def __init__(self, s_object, data_format, pk_chunking, instance_url, auth_header):
        self.s_object = s_object
        self.data_format = data_format
        self.pk_chunking = pk_chunking
        self.instance_url = instance_url
        self.auth_header = auth_header
        self.job_id = ''
        self.batches = []
        self.records_processed = 0
        self.records_written_to_csv = 0
        self.file_header = ''
        self.file_footer = ''
        self.file_output: str = constants.FILE_NAME_MAPPING[s_object]
        self.column_header_mapping = constants.COLUMN_HEADER_MAPPING[s_object]
        self.additional_column_mapping = constants.ADDITIONAL_COLUMN_MAPPING[s_object]
        self.processed_at = None
        self.create_job_payload = f'''
<?xml version="1.0" encoding="UTF-8"?>
<jobInfo
    xmlns="http://www.force.com/2009/06/asyncapi/dataload">
  <operation>query</operation>
  <object>{s_object}</object>
  <concurrencyMode>Parallel</concurrencyMode>
  <contentType>{data_format}</contentType>
</jobInfo>
'''
        self.close_job_payload = f'''
<?xml version="1.0" encoding="UTF-8"?>
<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
  <state>{constants.JOB_STATUS_CLOSED}</state>
</jobInfo>
'''

    def __enter__(self):
        endpoint = f'{self.instance_url}/services/async/60.0/job'
        headers = {**self.auth_header, **{
            'Content-Type': 'application/xml; charset=UTF-8',
            'Sforce-Enable-PKChunking': str(self.pk_chunking)
        }}
        response = self.post(endpoint, data=self.create_job_payload, headers=headers)
        root = ElementTree.fromstring(response.content)
        for child in root:
            if child.tag.endswith('id'):
                self.job_id = child.text
                break
        assert(self.job_id != '')
        print(f'INFO: Created Salesforce job {self.job_id}.')
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}'
        headers = {**self.auth_header, **{
            'Content-Type': 'text/csv; charset=UTF-8'
        }}
        response = self.post(endpoint, data=self.close_job_payload, headers=headers)
        root = ElementTree.fromstring(response.content)
        job_status = ''
        for child in root:
            if child.tag.endswith('state'):
                job_status = child.text
                break
        assert(job_status == constants.JOB_STATUS_CLOSED)
        print(f'INFO: Salesforce job {self.job_id} is {job_status}.')
        print(constants.LOG_SEPARATOR)

    def submit_query(self, query):
        endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}/batch'
        headers = {**self.auth_header, **{
            'Content-Type': 'text/csv; charset=UTF-8'
        }}
        response_job_id = ''
        response = self.post(endpoint, data=query, headers=headers)
        root = ElementTree.fromstring(response.content)
        for child in root:
            if child.tag.endswith('jobId'):
                response_job_id = child.text
                break
        assert(self.job_id == response_job_id)
        print(f'INFO: Submitted {self.s_object} query to job.')
        print(constants.LOG_SEPARATOR)

    def is_complete(self) -> bool:
        counter = 0
        endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}'
        number_batches_completed = 0
        number_batches_total = 0
        number_records_processed = 0
        while True:
            response = self.get(endpoint, headers=self.auth_header)
            root = ElementTree.fromstring(response.content)
            for child in root:
                if child.tag.endswith('numberBatchesCompleted'):
                    number_batches_completed = int(child.text)
                elif child.tag.endswith('numberBatchesTotal'):
                    number_batches_total = int(child.text)
                elif child.tag.endswith('numberRecordsProcessed'):
                    number_records_processed = int(child.text)
            print(f'INFO: Querying records using batch {number_batches_completed} out of {number_batches_total}, progress {int(number_batches_completed*100/number_batches_total)}%.', end='\r')
            
            if number_batches_total == 0:
                print(f'\nWARN: Zero batches are created for job {self.job_id}!')
                return False
            elif number_batches_total == number_batches_completed:
                self.processed_at = datetime.now()
                print(f'\nINFO: Total records queried {number_records_processed}.')
                return True
            
            counter += 1
            if counter >= constants.MAX_RETRY_COUNT:
                print(f'\nERROR: Maximum timeout reached while checking for Salesforce progress for job {self.job_id}!')
                return False
            time.sleep(constants.API_POLL_FREQ_SECONDS)

    def get_complete_batches(self):
        endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}/batch'
        response = self.get(endpoint, headers=self.auth_header)
        root = ElementTree.fromstring(response.content)
        for batch_info in root:
            batch = {}
            for child in batch_info:
                if child.tag.endswith('state'):
                    batch['state'] = child.text
                elif child.tag.endswith('id'):
                    batch['id'] = child.text
                elif child.tag.endswith('jobId'):
                    batch['job_id'] = child.text
                elif child.tag.endswith('numberRecordsProcessed'):
                    batch['number_records_processed'] = int(child.text)
            self.batches.append(batch)
        self.batches = list(filter((lambda x: x['state'] == constants.BATCH_STATUS_COMPLETED), self.batches))
        self.records_processed = reduce((lambda x, y: x + y), [batch['number_records_processed'] for batch in self.batches])
    
    def get_results(self):
        for index, batch in enumerate(self.batches):
            batch['results'] = []
            batch_id = batch['id']
            endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}/batch/{batch_id}/result'
            response = self.get(endpoint, headers=self.auth_header)
            root = ElementTree.fromstring(response.content)
            for result in root:
                batch['results'].append(result.text)
            print(f'INFO: Fetching results for batch {index + 1} out of {len(self.batches)}, progress {int((index+1)*100/len(self.batches))}%.', end='\r')
            time.sleep(constants.API_POLL_FREQ_SECONDS)
        print(f'\n{constants.LOG_SEPARATOR}')

    def generate_csv(self):
        header_generated = False
        self.file_output = self.file_output.replace('<FILE_SUFFIX>', self.processed_at.strftime('%Y%m%d%H%M%S'))
        self.file_output = self.file_output.replace('<JOB_ID>', self.job_id)
        for batch in self.batches:
            batch_id = batch['id']
            number_records_processed = batch['number_records_processed']
            for result in batch['results']:
                endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}/batch/{batch_id}/result/{result}'
                response = self.get(endpoint, headers=self.auth_header)
                rows = response.text.split('\n')
                if not header_generated:
                    self.write_file_header(rows[0])
                    header_generated = True
                if self.additional_column_mapping:
                    self.add_additional_columns(rows)
                table_data = '\n'.join(rows[1:])
                with open(self.file_output, '+at') as csvOutput:
                    csvOutput.write(table_data)
                time.sleep(constants.API_POLL_FREQ_SECONDS)
            self.records_written_to_csv += number_records_processed
            print(f'INFO: Writing {self.records_written_to_csv} records of {self.records_processed}, progress {int(self.records_written_to_csv*100/self.records_processed)}%.', end='\r')
        self.write_file_footer()
        print(f'\nINFO: Finished writing {self.file_output}.')
        print(constants.LOG_SEPARATOR)
    
    def write_file_header(self, column_header_row):
        with open(self.file_output, '+at') as csvOutput:
            csvOutput.write(f'"HEADER","ESMEE","{self.records_processed}","{self.s_object}"\n')
            csvOutput.write(self.generate_column_header(column_header_row, self.column_header_mapping))

    def generate_column_header(self, original_header: str, column_mapping) -> str:
        for key in column_mapping:
            original_header = original_header.replace(key, column_mapping[key])
        for key in constants.ADDITIONAL_COLUMN_MAPPING[self.s_object]:
            original_header += f',"{key}"'
        return original_header + '\n'
    
    def add_additional_columns(self, rows):
        for index, row in enumerate(rows):
            if index > 0 and index < len(rows)-1:
                for key in self.additional_column_mapping:
                    row += f',"{self.additional_column_mapping[key]}"'
            rows[index] = row    

    def write_file_footer(self):
        with open(self.file_output, '+at') as csvOutput:
            csvOutput.write(f'"FOOTER","{self.processed_at.strftime("%d-%m-%Y %H:%M:%S")}"')

    def get(self, endpoint, headers):
        response = requests.get(url=endpoint, headers=headers)
        response.raise_for_status()
        return response
        
    def post(self, endpoint, data, headers):
        response = requests.post(url=endpoint, data=data, headers=headers)
        response.raise_for_status()
        return response