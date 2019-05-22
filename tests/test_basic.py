import dqCountExtractor

import os
import boto3
import botocore
from unittest import mock, TestCase
from mock import call

from moto import mock_s3

MY_BUCKET = "my_bucket"
AWS_REGION = "eu-west-1"

MY_AWS_ACCESS_KEY_ID = "fake_access_key"
MY_AWS_ACCESS_KEY = "fake_secret_key"

START_DATE = "2017/07/25"
END_DATE = "2017/07/25"


@mock_s3
class DQCountExtractorTest(TestCase):
    def setUp(self):
        client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=MY_AWS_ACCESS_KEY_ID,
            aws_secret_access_key=MY_AWS_ACCESS_KEY,
        )
        try:
            s3 = boto3.resource(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=MY_AWS_ACCESS_KEY_ID,
                aws_secret_access_key=MY_AWS_ACCESS_KEY,
            )
            s3.meta.client.head_bucket(Bucket=MY_BUCKET)
        except botocore.exceptions.ClientError:
            pass
        else:
            err = "{bucket} should not exist.".format(bucket=MY_BUCKET)
            raise EnvironmentError(err)
        client.create_bucket(Bucket=MY_BUCKET)
        current_dir = os.path.dirname(__file__)
        fixtures_dir = os.path.join(current_dir, "fixtures")
        DQCountExtractorTest._upload_fixtures(MY_BUCKET, fixtures_dir)

    def tearDown(self):
        s3 = boto3.resource(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=MY_AWS_ACCESS_KEY_ID,
            aws_secret_access_key=MY_AWS_ACCESS_KEY,
        )
        bucket = s3.Bucket(MY_BUCKET)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

    def _upload_fixtures(bucket: str, fixtures_dir: str) -> None:
        client = boto3.client("s3")
        fixtures_paths = [
            os.path.join(path,  filename)
            for path, _, files in os.walk(fixtures_dir)
            for filename in files
        ]
        for path in fixtures_paths:
            key = os.path.relpath(path, fixtures_dir)
            if key.endswith(".zip"):
                client.upload_file(Filename=path, Bucket=bucket, Key=key)

    @mock.patch.dict(os.environ,{
        'AWS_SECRET_ACCESS_KEY':MY_AWS_ACCESS_KEY,
        'AWS_ACCESS_KEY_ID':MY_AWS_ACCESS_KEY_ID,
        'AWS_DEFAULT_REGION':AWS_REGION,
        'BUCKET':MY_BUCKET,
        'START_DATE':START_DATE,
        'END_DATE':END_DATE
    })
    @mock.patch('dqCountExtractor.print')
    def test_ConnectedToS3_InConstructor(self, mock_print):
        count_extractor = dqCountExtractor.DQCountExtractor()

        assert count_extractor.aws_access_key_id == MY_AWS_ACCESS_KEY_ID
        assert count_extractor.aws_secret_access_key == MY_AWS_ACCESS_KEY
        assert count_extractor.aws_region_name == AWS_REGION

        assert count_extractor.start_date == START_DATE
        assert count_extractor.end_date == END_DATE
        assert count_extractor.bucket == MY_BUCKET

        count_extractor.count_extractor(False)
        count_list = ['2017/07/25', 1, 4, 6112]
        mock_print.assert_called_with(count_list)

    @mock.patch.dict(os.environ,{
        'AWS_SECRET_ACCESS_KEY':MY_AWS_ACCESS_KEY,
        'AWS_ACCESS_KEY_ID':MY_AWS_ACCESS_KEY_ID,
        'AWS_DEFAULT_REGION':AWS_REGION,
        'BUCKET':MY_BUCKET,
        'START_DATE':START_DATE,
        'END_DATE':'2017/07/26'
    })
    @mock.patch('dqCountExtractor.print')
    def test_xmls_counted_when_nozip_for_one_date(self, mock_print):
        count_extractor = dqCountExtractor.DQCountExtractor()

        assert count_extractor.aws_access_key_id == MY_AWS_ACCESS_KEY_ID
        assert count_extractor.aws_secret_access_key == MY_AWS_ACCESS_KEY
        assert count_extractor.aws_region_name == AWS_REGION

        assert count_extractor.start_date == START_DATE
        assert count_extractor.end_date == '2017/07/26'
        assert count_extractor.bucket == MY_BUCKET

        count_extractor.count_extractor(False)

        calls = [ call(['2017/07/25', 1, 4, 6112]),call(['2017/07/26', 0, 0, 0])]

        mock_print.assert_has_calls(calls)
