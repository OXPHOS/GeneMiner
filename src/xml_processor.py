# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract disease type, disease stage and patient gender from xml files
and dump to Amazon Redshift

Author: Pan Deng

"""

from os import environ
from pyspark.sql import SparkSession, Row
import __credential__
import metainfo_loader


def process_xml():
    """
    Extract primary site, stage and gender information from clinical XML file and dump to Redshift
     
    """
    # Load clinical.xml file from Amazon S3
    filelist = metainfo_loader.file_loader(spark, "xml_list")
    cnt = 0
    print("Progress: every 100 processing")

    for f in filelist:
        # Load clinical.xml file from Amazon S3
        xml_schema = spark.read.format('com.databricks.spark.xml') \
            .options(rowTag='brca:patient') \
            .load("s3a://gdcdata/datasets/%s" % f.filepath)
        try:
            # TODO: Figure out why some xml cannot be processed
            # TODO: dump AVAILABLE fields to redshift based on f.case_id
            # Extract useful information
            xml_schema_rdd = xml_schema.rdd.map(lambda x: Row( \
                stage=x['shared_stage:stage_event']['shared_stage:pathologic_stage']._VALUE, \
                primary_site=x['clin_shared:tumor_tissue_site']._VALUE, \
                gender=x['shared:gender']._VALUE))
            # print("Entries producted:  ", xml_schema_rdd.count())
        except:
            print("\nExtractionError! %s \n" % f.filepath)

        cnt += 1
        if not cnt % 100:
            print('.', end=' ')


if __name__ == "__main__":
    # Include spark-xml package
    environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 \
                        --jars /usr/local/spark/jars/postgresql-42.2.2.jar pyspark-shell'

    # Setup python path for worker nodes
    environ['PYSPARK_PYTHON'] = '/home/ubuntu/anaconda3/bin/python'
    environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/anaconda3/bin/jupyter'

    spark = SparkSession \
        .builder \
        .master(__credential__.spark_host) \
        .appName("xml_reader") \
        .getOrCreate()

    process_xml()
    spark.stop()
