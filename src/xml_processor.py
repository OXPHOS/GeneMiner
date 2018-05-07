# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract disease type, disease stage and patient gender from xml files
and dump to PostgreSQL on Amazon RDS / Amazon Redshift

Author: Pan Deng

"""

from os import environ
from pyspark.sql import SparkSession
import psycopg2
import __credential__
import database_connector

psql=True


def update_patient_info(vals, key):
    """
    Update column values based on case_id in the same row
    
    :param vals: the values to be updated
    :param key: The key of the row to be updated
    """
    cur.execute("""
        UPDATE patient_info
        SET disease_stage='%s',
        disease_type='%s',
        gender='%s'
        WHERE case_id='%s';
        """ % (vals['stage'], vals['primary_site'], vals['gender'], key))
    conn.commit()


def process_xml():
    """
    Extract primary site, stage and gender information from clinical XML file and dump to PostgreSQL or Redshift
     
    """
    # Acquire xml file list
    if psql:
        filelist = database_connector.psql_file_loader(spark, tbname="xml_list")
    else:
        filelist = database_connector.redshift_file_loader(spark, tbname="xml_list", tmpdir="xml_files")

    # Progress bar
    cnt = 0
    print("Progress: every 100 processing")

    for f in filelist:
        # Load clinical.xml file from Amazon S3
        xml_schema = spark.read.format('com.databricks.spark.xml') \
            .options(rowTag='brca:patient') \
            .load("s3a://gdcdata/datasets/%s" % f.filepath)

        try:
            # TODO: Figure out why some xml cannot be processed
            # Extract useful information
            info = {'stage': xml_schema.first()['shared_stage:stage_event']['shared_stage:pathologic_stage']._VALUE, \
                    'primary_site': xml_schema.first()['clin_shared:tumor_tissue_site']._VALUE, \
                    'gender': xml_schema.first()['shared:gender']._VALUE}
            update_patient_info(info, key=f.caseid)

        except:
            print("\nExtractionError! %s \n" % f.filepath)

        cnt += 1
        if not cnt % 100:
            print('.', end=' ')


if __name__ == "__main__":
    # Include spark-xml package and drivers
    if psql:
        print("Using PostgreSQL as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 \
                            --jars ./jars/postgresql-42.2.2.jar pyspark-shell'

        # Connect to PostgreSQL
        conn = psycopg2.connect(host=__credential__.host_psql, dbname=__credential__.dbname_psql,
                                user=__credential__.user_psql, password=__credential__.password_psql)
        cur = conn.cursor()

    else:
        print("Using Redshift as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 \
                            --jars ./jars/spark-redshift_2.11-3.0.0-preview1.jar \
                            --jars ./jars/spark-avro_2.11-4.0.0.jar \
                            --jars ./jars/RedshiftJDBC41-1.2.12.1017.jar \
                            --jars ./jars/minimal-json-0.9.5.jar pyspark-shell'

        # Connect to Redshift
        conn = psycopg2.connect(host=__credential__.host_redshift, dbname=__credential__.dbname_redshift,
                                user=__credential__.user_redshift, password=__credential__.password_redshift,
                                port=__credential__.port_redshift)
        cur = conn.cursor()

    # Setup python path for worker nodes
    environ['PYSPARK_PYTHON'] = '/home/ubuntu/anaconda3/bin/python'
    environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/anaconda3/bin/jupyter'

    # Start spark session
    spark = SparkSession \
        .builder \
        .master(__credential__.spark_host) \
        .appName("xml_reader") \
        .getOrCreate()

    process_xml()

    cur.close()
    conn.close()

    spark.stop()
