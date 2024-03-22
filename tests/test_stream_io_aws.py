"""
Test the ability of Tensorizer to read and write to AWS S3, when the credentials are not provided explicitly to Tensorizer.
boto3 will then seek credentials from all the usual places, for example from the EC2 Instance Metadata Service (IMDS).
This is useful for example with temporary credentials - instead of passing around the session token, we can expect boto3 to handle it.
In order for this test to work, you must ensure that boto3 has the necessary permissions and access to credentials.
The test will not run unless the environment variable AWS_S3_TEST_PATH is set.
"""
import unittest
import os

from tensorizer import stream_io
import boto3


class TestAWSS3(unittest.TestCase):
    AWS_S3_TEST_PATH = os.environ.get('AWS_S3_TEST_PATH')
    TEST_BYTES = b"Widening gyre"

    def remove_target_file(self):
        s3 = boto3.resource("s3")
        bucket_name = self.AWS_S3_TEST_PATH.split('/')[2]
        key = '/'.join(self.AWS_S3_TEST_PATH.split('/')[3:])
        s3.Object(bucket_name, key).delete()

    @unittest.skipUnless(AWS_S3_TEST_PATH is not None, "AWS_S3_TEST_PATH not defined, skipping AWS upload download test.")
    def test_s3_upload_download(self):

        self.remove_target_file()

        with stream_io.open_stream(
            self.AWS_S3_TEST_PATH,
            "wb",
            s3_access_key_id=None,
            s3_secret_access_key=None,
            s3_endpoint=None,
            s3_fallback_to_boto3_config=True,
        )  as s3_stream:

            s3_stream.write(self.TEST_BYTES)

        with stream_io.open_stream(
            self.AWS_S3_TEST_PATH,
            "rb",
            s3_access_key_id=None,
            s3_secret_access_key=None,
            s3_endpoint=None,
            s3_fallback_to_boto3_config=True,
        )  as s3_stream:

            redownloaded_stream = s3_stream.read()
            self.assertEqual(redownloaded_stream, self.TEST_BYTES)

        self.remove_target_file()


    def test_ensure_exception_without_s3_fallback_to_boto3_config(self):

        excepted = False

        try:

            test_stream = b"Hello World"
            with stream_io.open_stream(
                self.AWS_S3_TEST_PATH,
                "wb",
                s3_access_key_id=None,
                s3_secret_access_key=None,
                s3_endpoint=None,
                s3_fallback_to_boto3_config=False,
            )  as s3_stream:

                s3_stream.write(self.TEST_BYTES)

        except:
            excepted = True

        self.assertTrue(excepted, "Expected exception not raised when not unsetting s3_fallback_to_boto3_config with no creds (write)")

        excepted = False
        try:
            with stream_io.open_stream(
                self.AWS_S3_TEST_PATH,
                "rb",
                s3_access_key_id=None,
                s3_secret_access_key=None,
                s3_endpoint=None,
                s3_fallback_to_boto3_config=False,
            )  as s3_stream:

               s3_stream.read()
        except:
            excepted = True

        self.assertTrue(excepted, "Expected exception not raised when not unsetting s3_fallback_to_boto3_config with no creds (read)")

