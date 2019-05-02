import boto3
import datetime
from datetime import date, timedelta
import io
import os
import zipfile

import pathos.multiprocessing as mp


class DQCountExtractor(object):
    """Extracts xml counts from DQ for a range of given dates"""

    def __init__(self):

        # self.set_input()
        self.set_input_from_env()
        start_year, start_mon, start_day = self.start_date.split("/")
        end_year, end_mon, end_day = self.end_date.split("/")
        self.d1 = date(int(start_year), int(start_mon), int(start_day))
        d2 = date(int(end_year), int(end_mon), int(end_day))
        self.delta = d2 - self.d1

    def set_input_from_env(self):
        self.should_multi_process = os.environ['MULTI_PROCESS']  # Set it to 'true' if multi-processing required

        if self.should_multi_process == 'true':
            self.pool_size = int(os.environ['POOL_SIZE']) # Number of processes that should run in parallel

        self.aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        self.aws_region_name = os.environ['AWS_DEFAULT_REGION']

        self.bucket = os.environ['BUCKET']
        self.start_date = os.environ['START_DATE']
        self.end_date = os.environ['END_DATE']

    def count_extractor(self):
        session = boto3.Session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.
                                aws_secret_access_key, region_name=self.aws_region_name)

        print("AWS session started...")
        get_result_args = []
        for i in range(self.delta.days + 1):
            get_result_args.append((i,self.d1, session, self.bucket))

        return_list = []

        if self.should_multi_process != 'false':
            try:
                p = mp.Pool(self.pool_size)
                p_map = p.map(get_results, get_result_args)
                return_list = p_map
            finally:
                p.close()
        else:  # Test mode - no parallel processing
            for d in get_result_args:
                r = get_results(d)
                return_list.append(r)

        for e in return_list:
            print(e)


def get_results(t):
    day = t[0]
    start_date = t[1]
    aws_session = t[2]
    bucket_name = t[3]
    file_received_date = datetime.datetime.strptime(str(start_date + timedelta(day)), '%Y-%m-%d').strftime('%Y/%m/%d')

    s3 = aws_session.resource('s3')
    s3_bucket = s3.Bucket(bucket_name)

    filtered_objects = s3_bucket.objects.filter(Prefix='s4/parsed/' + str(file_received_date) + '/')
    return get_counts(filtered_objects, file_received_date)


def get_counts(filtered_objects, file_received_date):
    zip_file_counter = 0
    xml_file_counter = 0
    uncompressed_xml_size = 0
    for filtered_object in filtered_objects:
        if filtered_object.key.endswith('.zip'):
            zip_file_counter = zip_file_counter + 1
            with io.BytesIO(filtered_object.get()["Body"].read()) as tf:
                tf.seek(0)
                with zipfile.ZipFile(tf, mode='r') as zip_file:
                    for xml_info in zip_file.infolist():
                        if ".xml" in str(xml_info):
                            xml_file_counter = xml_file_counter + 1
                            deflate_file_size = xml_info.file_size
                            uncompressed_xml_size = uncompressed_xml_size + deflate_file_size
    count_list = [file_received_date, zip_file_counter, xml_file_counter, uncompressed_xml_size]
    return count_list


if __name__ == '__main__':
    DQCountExtractor().count_extractor()
