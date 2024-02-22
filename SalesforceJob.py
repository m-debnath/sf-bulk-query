from datetime import datetime
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
        response = requests.post(endpoint, data=self.create_job_payload, headers=headers)
        root = ElementTree.fromstring(response.content)
        for child in root:
            if child.tag.endswith('id'):
                self.job_id = child.text
                break
        assert(self.job_id != '')
        print(f'INFO: ----------------------------------------')
        print(f'INFO: Created Salesforce job {self.job_id}.')
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}'
        headers = {**self.auth_header, **{
            'Content-Type': 'text/csv; charset=UTF-8'
        }}
        response = requests.post(endpoint, data=self.close_job_payload, headers=headers)
        root = ElementTree.fromstring(response.content)
        job_status = ''
        for child in root:
            if child.tag.endswith('state'):
                job_status = child.text
                break
        assert(job_status == constants.JOB_STATUS_CLOSED)
        print(f'INFO: ----------------------------------------')
        print(f'INFO: Salesforce job {self.job_id} is {job_status}.')

    def submit_query(self, query):
        endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}/batch'
        headers = {**self.auth_header, **{
            'Content-Type': 'text/csv; charset=UTF-8'
        }}
        response_job_id = ''
        response = requests.post(endpoint, data=query, headers=headers)
        root = ElementTree.fromstring(response.content)
        for child in root:
            if child.tag.endswith('jobId'):
                response_job_id = child.text
                break
        assert(self.job_id == response_job_id)
        print(f'INFO: Submitted query to job {self.job_id}.')

    def is_complete(self) -> bool:
        counter = 0
        endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}'
        number_batches_completed = 0
        number_batches_total = 0
        number_records_processed = 0
        print(f'INFO: ----------------------------------------')
        while True:
            response = requests.get(endpoint, headers=self.auth_header)
            root = ElementTree.fromstring(response.content)
            for child in root:
                if child.tag.endswith('numberBatchesCompleted'):
                    number_batches_completed = int(child.text)
                    print(f'INFO: Batches completed: {number_batches_completed}')
                elif child.tag.endswith('numberBatchesTotal'):
                    number_batches_total = int(child.text)
                    print(f'INFO: Total batches: {number_batches_total}')
                elif child.tag.endswith('numberRecordsProcessed'):
                    number_records_processed = int(child.text)
                    print(f'INFO: Records processed: {number_records_processed}')
            
            if number_batches_total == 0:
                print(f'WARN: ----------------------------------------')
                print(f'WARN: Zero batches are created for job {self.job_id}!')
                return False
            elif number_batches_total == number_batches_completed:
                self.processed_at = datetime.now()
                print(f'INFO: ----------------------------------------')
                print(f'INFO: All batches are processed for job {self.job_id}.')
                return True
            
            counter += 1
            if counter >= constants.MAX_RETRY_COUNT:
                print(f'ERROR: ----------------------------------------')
                print(f'ERROR: Maximum timeout reached while checking for Salesforce progress for job {self.job_id}!')
                return False
            time.sleep(constants.API_POLL_FREQ_SECONDS)

    def get_complete_batches(self):
        endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}/batch'
        response = requests.get(endpoint, headers=self.auth_header)
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
                    self.records_processed += batch['number_records_processed']
            self.batches.append(batch)
        self.batches = [batch for batch in self.batches if batch['state'] == constants.BATCH_STATUS_COMPLETED]
        print(f'INFO: ----------------------------------------')
        print(f'INFO: Total number of complete batches: {len(self.batches)}')
        print(f'INFO: Total number of processed records: {self.records_processed}')
        return self
    
    def get_results(self):
        print(f'INFO: ----------------------------------------')
        print(f'INFO: Fetching results...')
        for batch in self.batches:
            batch['results'] = []
            batch_id = batch['id']
            endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}/batch/{batch_id}/result'
            response = requests.get(endpoint, headers=self.auth_header)
            root = ElementTree.fromstring(response.content)
            for result in root:
                batch['results'].append(result.text)
            time.sleep(constants.API_POLL_FREQ_SECONDS)
        print(f'INFO: Done.')
        return self
    
    def generate_csv(self):
        header_generated = False
        self.file_output = self.file_output.replace('<FILE_SUFFIX>', self.processed_at.strftime('%Y%m%d%H%M%S'))
        self.file_output = self.file_output.replace('<JOB_ID>', self.job_id)
        print(f'INFO: ----------------------------------------')
        for batch in self.batches:
            batch_id = batch['id']
            number_records_processed = batch['number_records_processed']
            for result in batch['results']:
                endpoint = f'{self.instance_url}/services/async/60.0/job/{self.job_id}/batch/{batch_id}/result/{result}'
                response = requests.get(endpoint, headers=self.auth_header)
                rows = response.text.split('\n')
                if not header_generated:
                    self.file_header = f'"HEADER","ESMEE","{self.records_processed}","{self.s_object}"\n'
                    with open(self.file_output, '+at') as csvOutput:
                        csvOutput.write(self.file_header)
                        csvOutput.write(self.generate_column_header(rows[0], self.column_header_mapping))
                        header_generated = True
                if constants.ADDITIONAL_COLUMN_MAPPING[self.s_object]:
                    self.add_additional_columns(rows)
                table_data = '\n'.join(rows[1:])
                with open(self.file_output, '+at') as csvOutput:
                    csvOutput.write(table_data)
                time.sleep(constants.API_POLL_FREQ_SECONDS)
            self.records_written_to_csv += number_records_processed
            print(f'INFO: Wrote {self.records_written_to_csv} records to {self.file_output}.')
        with open(self.file_output, '+at') as csvOutput:
            self.file_footer = f'"FOOTER","{self.processed_at.strftime("%d-%m-%Y %H:%M:%S")}"'
            csvOutput.write(self.file_footer)
        return self
    
    def generate_column_header(self, original_header: str, column_mapping) -> str:
        for key in column_mapping:
            original_header = original_header.replace(key, column_mapping[key])
        for key in constants.ADDITIONAL_COLUMN_MAPPING[self.s_object]:
            original_header += f',"{key}"'
        return original_header + '\n'
    
    def add_additional_columns(self, rows):
        for index, row in enumerate(rows):
            if index > 0 and index < len(rows)-1:
                for key in constants.ADDITIONAL_COLUMN_MAPPING[self.s_object]:
                    row += f',"{constants.ADDITIONAL_COLUMN_MAPPING[self.s_object][key]}"'
            rows[index] = row