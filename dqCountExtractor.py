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

        self.aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        self.aws_region_name = os.environ['AWS_DEFAULT_REGION']

        self.bucket = os.environ['BUCKET']
        self.start_date = os.environ['START_DATE']
        self.end_date = os.environ['END_DATE']
        self.date_separator = "/"
        if self.start_date.find("-") > 0 and self.end_date.find("-") > 0:
            self.date_separator = "-"
        start_year, start_mon, start_day = self.start_date.split(self.date_separator)
        end_year, end_mon, end_day = self.end_date.split(self.date_separator)
        self.d1 = date(int(start_year), int(start_mon), int(start_day))
        d2 = date(int(end_year), int(end_mon), int(end_day))
        self.delta = d2 - self.d1

    def count_extractor(self, should_multi_process=True):
        session = boto3.Session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.
                                aws_secret_access_key, region_name=self.aws_region_name)

        print("AWS session started...")
        get_result_args = []
        for i in range(self.delta.days + 1):
            get_result_args.append((i, self.d1, session, self.bucket, self.date_separator))
        return_list = []

        if should_multi_process:
            pool_size = int(os.environ['POOL_SIZE']) # Number of processes that should run in parallel
            try:
                p = mp.Pool(pool_size)
                p_map = p.map(get_results, get_result_args)
                return_list = p_map
            finally:
                p.close()
        else:  # Test mode - no parallel processing
            for d in get_result_args:
                r = get_results(d)
                return_list.append(r)

        # for e in return_list:
        #     print(e)


def get_results(t):
    day = t[0]
    start_date = t[1]
    aws_session = t[2]
    bucket_name = t[3]
    date_separator = t[4]
    if date_separator == "/":
        file_received_date = datetime.datetime.strptime(str(start_date + timedelta(day)), '%Y-%m-%d').\
            strftime('%Y/%m/%d')
        key_prefix = 's4/parsed/' + str(file_received_date) + '/'
    else:
        file_received_date = datetime.datetime.strptime(str(start_date + timedelta(day)), '%Y-%m-%d').\
            strftime('%Y-%m-%d')
        key_prefix = 'parsed/' + str(file_received_date) + '/'
    s3 = aws_session.resource('s3')
    s3_bucket = s3.Bucket(bucket_name)
    filtered_objects = s3_bucket.objects.filter(Prefix=key_prefix)
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
    print(count_list)
    return count_list


if __name__ == '__main__':
    DQCountExtractor().count_extractor()
