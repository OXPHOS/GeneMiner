# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Unzip .gz files stored at Amazon S3 with Boto3

Convert boto3.streamingbody to bytestream that is compatible with gzip:
https://gist.github.com/veselosky/9427faa38cee75cd8e27

Author: Pan Deng

"""

import boto3
from io import BytesIO
from gzip import GzipFile


class S3File:
    """
    Wraps filename and meta information to get file from Amazon S3 bucket
    """
    def __init__(self, name, metadata):
        self.name = name
        self.metadata = metadata


class S3BatchUnzipper:
    """
    Setup S3 connector
    Extract all .gz files from given path
    Unzip the .gz files and return file
    """
    def __init__(self, bucket, targetdir):
        resource = boto3.resource('s3')  # high-level object-oriented API
        print("- Connecting to bucket: [%s] ..." % bucket)
        self.bucket = resource.Bucket(bucket)  # subsitute this for your s3 bucket name.
        self.prefix = targetdir
        print("- Acquiring .gz files from path: [%s]" % self.prefix)
        self.file_list = self._get_gzip_file_list()

    def _get_gzip_file_list(self):
        """
        Extract all .gz files from given path
        
        :return: .gz file list in given path
        """

        # Extract all file information under path of prefix
        file_summary = list(self.bucket.objects.filter(Prefix=self.prefix))
        # Extract file metadata and generate S3File with inforamtion: file_name, file_metadata
        files = list(map(lambda x: S3File(x.key, x.get()), file_summary))
        # Filter all .gz files
        return [f for f in files if f.metadata['ContentType'] == 'application/x-gzip']

    def get_gzip_file_list(self):
        return self.file_list

    @staticmethod
    def unzip_file(file):
        """
        Unzip .gz file with gzip
        :param file: the .gz file
        :return: unzipped file
        """
        obj = file['Body']  # type(obj) = class 'botocore.response.StreamingBody'
        bytestream = BytesIO(obj.read())
        return GzipFile(None, 'r', fileobj=bytestream)

    def unzip_batch(self):
        # TODO: Iteratively unzip and yield the unzipped file by calling static unzip_file()
        pass


if __name__=='__main__':
    # For test
    unzipper = S3BatchUnzipper(bucket='gdcdata', targetdir='homo_sapiens/')
    files = unzipper.get_gzip_file_list()
    print(len(files))
    for file in files:
        f = S3BatchUnzipper.unzip_file(file.metadata)
        print(file.name)
        for line in f:
            print(line)
        break
