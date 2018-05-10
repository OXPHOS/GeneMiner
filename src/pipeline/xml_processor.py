# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract disease type, disease stage and patient gender from xml files
and dump to PostgreSQL on Amazon RDS / Amazon Redshift

Author: Pan Deng

"""

from os import environ
from pyspark.sql import SparkSession
import boto3
import xml.etree.ElementTree as ET
import psycopg2
import time
import __credential__
import database_connector

psql=True
test=True

# To extract xml field text from xml tree structure
xml_ref = {'stage': './/*/{http://tcga.nci/bcr/xml/clinical/shared/stage/2.7}pathologic_stage', \
           'primary_site': './/{http://tcga.nci/bcr/xml/clinical/shared/2.7}tumor_tissue_site', \
           'gender': './/{http://tcga.nci/bcr/xml/shared/2.7}gender'}


def extract_field(files):
    """
    Access XML files on Amazon S3 with Boto3 and xml.etree with paths given in RDD
    And return extracted field
    
    :param files: RDD of filenames and path to files
    :return: yield extracted information from each file
    """
    resource = boto3.resource('s3', aws_access_key_id=__credential__.aws_access_key_id, \
                              aws_secret_access_key=__credential__.aws_secret_access_key)
    for f in files:
        # Stream-in files from S3
        obj = resource.Object('gdcdata', 'datasets/%s' % f.filepath)
        body = obj.get()['Body'].read()

        # Extract information: stage, primary site and gender associated with patient id,
        info = dict(map(lambda x: (x[0], ET.fromstring(body).find(x[1]).text), xml_ref.items()))
        info.update({"caseid": f.caseid})

        yield info


def update_patient_info(rows):
    """
    Update column values based on case id in the same row

    :param rows: the partition of RDD to be updated in the database
    """
    from psycopg2 import extras
    if psql:
        # Connect to PostgreSQL
        conn = psycopg2.connect(host=__credential__.host_psql, dbname=__credential__.dbname_psql,
                                user=__credential__.user_psql, password=__credential__.password_psql)
    else:
        # Connect to Redshift
        conn = psycopg2.connect(host=__credential__.host_redshift, dbname=__credential__.dbname_redshift,
                                user=__credential__.user_redshift,
                                password=__credential__.password_redshift,
                                port=__credential__.port_redshift)
    cur = conn.cursor()

    # Write rows to table in database
    query = """
            UPDATE patient_info
            SET disease_stage=%(stage)s,
            disease_type=%(primary_site)s,
            gender=%(gender)s
            WHERE case_id=%(caseid)s;
        """
    psycopg2.extras.execute_batch(cur, query, rows)
    conn.commit()
    '''
    for row in rows:
        cur.execute("""
            UPDATE patient_info
            SET disease_stage='%s',
            disease_type='%s',
            gender='%s'
            WHERE case_id='%s';
        """ % (row['stage'], row['primary_site'], row['gender'], row['caseid']))
    conn.commit()
    '''
    cur.close()
    conn.close()


def process_xml():
    """
    Extract primary site, stage and gender information from clinical XML file and dump to PostgreSQL or Redshift
     
    """
    # Acquire xml file list
    if psql:
        filelist_rdd = database_connector.psql_file_loader(spark, tbname="xml_list")
    else:
        filelist_rdd = database_connector.redshift_file_loader(spark, tbname="xml_list", tmpdir="xml_files")

    if test:
        start_time = time.time()
        # filelist = filelist.take(100)

    # Extract required fields from xml files
    xml_schema_rdd = filelist_rdd.mapPartitions(extract_field)

    # Update the database
    xml_schema_rdd.foreachPartition(update_patient_info)

    if test:
        print("TOTAL RUNNING TIME: ", (time.time() - start_time))


if __name__ == "__main__":
    # Include spark-xml package and drivers
    if psql:
        print("Using PostgreSQL as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 \
                            --jars ./jars/postgresql-42.2.2.jar pyspark-shell'
    else:
        print("Using Redshift as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 \
                            --jars ./jars/spark-redshift_2.11-3.0.0-preview1.jar \
                            --jars ./jars/spark-avro_2.11-4.0.0.jar \
                            --jars ./jars/RedshiftJDBC41-1.2.12.1017.jar \
                            --jars ./jars/minimal-json-0.9.5.jar pyspark-shell'

    # Setup python path for worker nodes
    environ['PYTHONPATH'] = '$PYTHONPATH:/home/ubuntu/GeneMiner/src'
    environ['PYSPARK_PYTHON'] = '/home/ubuntu/anaconda3/bin/python'
    environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/anaconda3/bin/jupyter'

    # Start spark session
    spark = SparkSession \
        .builder \
        .master(__credential__.spark_host) \
        .appName("xml_reader") \
        .getOrCreate()

    # IMPORTANT: to import module in the same python package
    spark.sparkContext.addPyFile('/home/ubuntu/GeneMiner/src/__credential__.py')

    process_xml()

    spark.stop()
