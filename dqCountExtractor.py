import boto3
import datetime
from datetime import date, timedelta
from multiprocessing import Process, Pipe
import io
import re
import os
import zipfile


class DQCountExtractor(object):
    """Extracts xml counts from DQ for a range of given dates"""

    def __init__(self):
        self.aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        self.aws_region_name = os.environ['AWS_DEFAULT_REGION']
        self.bucket = os.environ['BUCKET']
        self.start_date = os.environ['START_DATE']
        self.end_date = os.environ['END_DATE']
        self.session = boto3.Session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.
                                     aws_secret_access_key, region_name=self.aws_region_name)
        self.s3 = self.session.resource('s3')

    def count_extractor(self):
        bucket = self.s3.Bucket(self.bucket)
        start_year, start_mon, start_day = self.start_date.split("/")
        end_year, end_mon, end_day = self.end_date.split("/")
        d1 = date(int(start_year), int(start_mon), int(start_day))
        d2 = date(int(end_year), int(end_mon), int(end_day))
        delta = d2 - d1

        processes = []

        parent_connections = []

        for i in range(delta.days + 1):
            file_received_date = datetime.datetime.strptime(str(d1 + timedelta(i)), '%Y-%m-%d').strftime('%Y/%m/%d')
            filtered_objects = bucket.objects.filter(Prefix='s4/parsed/' + str(file_received_date) + '/')
            parent_conn, child_conn = Pipe()
            parent_connections.append(parent_conn)
            process = Process(target=get_counts, args=(filtered_objects, file_received_date, child_conn))
            processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()


def get_counts(filtered_objects, file_received_date, conn):
    zip_file_counter = 0
    xml_file_counter = 0
    uncompressed_xml_size = 0
    for filtered_object in filtered_objects:
        if filtered_object.get()['ContentType'] == 'application/zip':
            zip_file_counter = zip_file_counter + 1
            with io.BytesIO(filtered_object.get()["Body"].read()) as tf:
                tf.seek(0)
                with zipfile.ZipFile(tf, mode='r') as zip_file:
                    for xml_info in zip_file.infolist():
                        if ".xml" in str(xml_info):
                            xml_file_counter = xml_file_counter + 1
                            deflate_file_size = int(re.search('deflate file_size=(.+?) compress_size', str(xml_info))
                                                    .group(1))
                            uncompressed_xml_size = uncompressed_xml_size + deflate_file_size
    count_list = [file_received_date, zip_file_counter, xml_file_counter, uncompressed_xml_size]
    print(count_list)
    conn.close()


DQCountExtractor().count_extractor()

