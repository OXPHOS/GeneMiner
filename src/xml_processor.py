# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract disease type, disease stage and patient gender from xml files
and dump to PostgreSQL on Amazon RDS / Amazon Redshift

Author: Pan Deng

"""

from os import environ
from pyspark.sql import SparkSession, Row
import psycopg2
import time
import __credential__
import database_connector

psql=True
test=True


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

    if test:
        start_time = time.time()
        filelist = filelist[:100]

    for f in filelist:
        # Load clinical.xml file from Amazon S3
        xml_schema = spark.read.format('com.databricks.spark.xml') \
            .options(rowTag='brca:patient') \
            .load("s3a://gdcdata/datasets/%s" % f.filepath)

        try:
            # Extract useful information and update the database
            def update_patient_info(rows):
                """
                Update column values based on case id in the same row

                :param rows: the partition of RDD to be updated in the database
                """
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
                for row in rows:
                    cur.execute("""
                        UPDATE patient_info
                        SET disease_stage='%s',
                        disease_type='%s',
                        gender='%s'
                        WHERE case_id='%s';
                    """ % (row['stage'], row['primary_site'], row['gender'], f.caseid))
                conn.commit()

            # TODO: Figure out why some xml cannot be processed
            xml_schema_rdd = xml_schema.rdd.map(lambda x: Row( \
                stage=x['shared_stage:stage_event']['shared_stage:pathologic_stage']._VALUE, \
                primary_site=x['clin_shared:tumor_tissue_site']._VALUE, \
                gender=x['shared:gender']._VALUE))

            xml_schema_rdd.foreachPartition(update_patient_info)

        except:
            print("\nExtractionError! %s \n" % f.filepath)

        cnt += 1

    if test:
        print("TOTAL RUNNING TIME on 100 FILES: ", (time.time() - start_time))
    


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

    spark.sparkContext.addPyFile('/home/ubuntu/GeneMiner/src/__credential__.py')
    process_xml()

    #cur.close()
    #conn.close()

    spark.stop()
