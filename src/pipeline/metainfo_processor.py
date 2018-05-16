# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Integrate metadata JSON file and manifest file, and export to PostgreSQL

Data source: https://portal.gdc.cancer.gov/, NCI

Author: Pan Deng

"""

from pyspark.sql import SparkSession
from os import environ
import psycopg2
import __credential__
import database_connector

TableByFormat = {'BCR XML': 'xml_list', 'TXT': 'txt_list'}
psql=True


def create_table():
    """
    Create a new PostgreSQL or Redshift table to record patient information
    The table columns can be altered via SQL commands after creation
    """
    if psql:
        conn = psycopg2.connect(host=__credential__.host_psql, dbname=__credential__.dbname_psql,
                                user=__credential__.user_psql, password=__credential__.password_psql)
    else:
        conn = psycopg2.connect(host=__credential__.host_redshift, dbname=__credential__.dbname_redshift,
                                user=__credential__.user_redshift, password=__credential__.password_redshift,
                                port=__credential__.port_redshift)
    cur = conn.cursor()
    cur.execute("""
            DROP TABLE IF EXISTS patient_info""")
    conn.commit()

    cur.execute("""
    CREATE TABLE patient_info(
        case_id text PRIMARY KEY,
        project_id text,
        disease_type text,
        disease_stage text,
        gender text,
        UNIQUE (case_id)
    )
    """)
    # case_id is patient id
    # TODO: Figure out how to make the unique unique
    conn.commit()


def main():
    create_table()

    # Read meta data file, with information about patient ID, project info and filename
    meta = spark.read.json('s3a://gdcdata/refs/files.c+r.json', multiLine=True)
    meta.createOrReplaceTempView("meta_view")

    # Read manifest data file, with information about the directory the files are in
    manifest = spark.read.format("csv") \
        .option("delimiter", "\t").option("quote", "") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load('s3a://gdcdata/refs/gdc_manifest.c+r.txt')
    manifest = manifest.selectExpr('id as path', 'filename as filename')
    manifest.createOrReplaceTempView("manifest_view")

    # Join 2 tables on file name, and get: filename, patientid, projectid, data format and filepath
    index = spark.sql('''
                SELECT manifest_view.path, manifest_view.filename, meta_view.data_format, \
                concat_ws('', meta_view.cases.project.project_id) AS project_id,
                concat_ws('', meta_view.cases.case_id) AS case_id
                FROM manifest_view
                INNER JOIN meta_view ON  manifest_view.filename=meta_view.file_name
            ''')  # Add concat_ws to solve the nested array structure issue that prevents writing to db

    # Add non-duplicated patient information to patient_info table
    index.createOrReplaceTempView("index_view")
    if psql:
        spark.sql('''SELECT DISTINCT case_id, project_id FROM index_view''') \
            .write.format("jdbc") \
            .option("url", 'jdbc:postgresql://%s' % __credential__.jdbc_accessible_host_psql) \
            .option("dbtable", "patient_info") \
            .option('user', __credential__.user_psql) \
            .option('password', __credential__.password_psql) \
            .mode("append") \
            .save()
    else:
        spark.sql('''SELECT DISTINCT case_id, project_id FROM index_view''') \
            .write.format("com.databricks.spark.redshift") \
            .option("url", __credential__.jdbc_accessible_host_redshift) \
            .option("dbtable", "patient_info") \
            .option("forward_spark_s3_credentials", True) \
            .option("tempdir", "s3n://gdcdata/tmp_create_table") \
            .mode("append") \
            .save()


    # Split files and save to PostgreSQL
    # Group files by column: data_format
    files_groupby_types = list(map(
        lambda key: {'type': key,'flist': index.filter(index.data_format == key)},
        TableByFormat))
    for files in files_groupby_types:
        print("Saving [%s] data to PostgreSQL table [%s]..." \
              % (files['type'], TableByFormat[files['type']]))
        if psql:
            database_connector.psql_saver(spark, df=files['flist'], tbname=TableByFormat[files['type']], \
                                          savemode='overwrite')
        else:
            database_connector.redshift_saver(spark, df=files['flist'], tbname=TableByFormat[files['type']], \
                                              tmpdir='tmp_filelist', savemode='overwrite')

    # Save unreadable files
    unreadable = index.rdd.filter(lambda x: x.data_format not in TableByFormat)
    if unreadable.count():
        if psql:
            print("Saving data in unknown foramt to PostgreSQL table: unknowns.")
            database_connector.psql_saver(spark, df=unreadable.toDF(), tbname='unknowns', savemode='overwrite')
        else:
            print("Saving data in unknown foramt to Redshift table: unknowns.")
            database_connector.redshift_saver(spark, df=unreadable.toDF(), tbname='unknowns', \
                                              tmpdir='tmp_unknown', savemode='overwrite')

    # cursor.close()
    # conn.close()


if __name__ == "__main__":
    # Setup Driver for connection
    if psql:
        print("Using PostgreSQL as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./jars/postgresql-42.2.2.jar pyspark-shell'
    else:
        print("Using Redshift as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./spark-redshift_2.10-3.0.0-preview1.jar \
                                --jars ./spark/jars/spark-avro_2.11-4.0.0.jar \
                                --jars ./spark/jars/RedshiftJDBC41-1.2.12.1017.jar pyspark-shell'

    # Setup python path for worker nodes
    environ['PYSPARK_PYTHON'] = '/home/ubuntu/anaconda3/bin/python'
    environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/anaconda3/bin/jupyter'

    spark = SparkSession \
        .builder \
        .master(__credential__.spark_host) \
        .appName("meta_info_processor") \
        .getOrCreate()

    main()

    spark.stop()
