# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract rna-seq information from tabular text files
and dump to PostgreSQL on Amazon RDS / Amazon Redshift

Author: Pan Deng

"""

from os import environ
from pyspark.sql import SparkSession, Row
import psycopg2
import __credential__
import database_connector
from py4j.protocol import Py4JJavaError

psql=True


def load_reference_genome(spark):
    """
    Read reference genome data pre-stored on PostgreSQL to DataFrame
    
    :param spark: SparkSession
    :return: reference DataFrame 
    """
    if psql:
        ref_genome = database_connector.psql_loader(spark, tbname=__credential__.dbtable_psql)
    else:
        pass
        # To be implemented
        # ref_genome = database_connector.redshift_loader(spark, tbname='', tmpdir='ref_genome')

    # Truncate the gene id from for eg. `ENSG00000007080.9` to `ENSG00000007080`
    ref_genome_trunc = ref_genome.rdd \
        .map(lambda x: Row(gene_id=x.id.split('.')[0], gene_name=x.name)) \
        .toDF()

    return ref_genome_trunc


def update_patient_info(val, key):
    """
    Update column values based on case_id in the same row

    :param vals: the values to be updated
    :param key: The key of the row to be updated
    """
    cur.execute("""
        UPDATE patient_info
        SET gene_expr_table='%s'
        WHERE case_id='%s';
        """ % (val, key))
    conn.commit()


def process_rnaseq(spark, ref):
    """
    Convert rnaseq FPKM-UQ data to readable tabular dataset with reference genome and dump to PostgreSQL or Redshift
    
    :param spark: SparkSession
    :param ref: reference genome with type DataFrame 
    """
    # Acquire RNA-seq file list
    if psql:
        filelist = database_connector.psql_file_loader(spark, tbname="txt_list")
    else:
        filelist = database_connector.redshift_file_loader(spark, tbname="txt_list", tmpdir="txt_files")

    # Progress br
    cnt = 0
    print("Progress: every 100 processing")

    for f in filelist:
        # There are 3 types of RNAseq files representing similar results.
        # So only process -UQ files here
        if '-UQ.txt' not in f.filepath:
            continue

        try:
            # In tabular format of: [gene_id \t expression_value]
            fpkm = spark.read.format("csv") \
                .option("delimiter", "\t").option("quote", "") \
                .option("header", "false") \
                .option("inferSchema", "true") \
                .load("s3a://gdcdata/datasets/%s" % f.filepath)

            # Filter all genes with expression level == 0
            fpkm = fpkm.filter(fpkm._c1>0)

            # Truncate the gene id from for eg. `ENSG00000007080.9` to `ENSG00000007080`
            fpkm_trunc = fpkm.rdd \
                .map(lambda x: Row(gene_id=x[0].split('.')[0], expr_val=x[1])) \
                .toDF()

            # Join to reference genome to get table: gene_id | gene_name | expr_val
            # TODO: Map-side join
            match = fpkm_trunc.join(ref, 'gene_id')

            # '-' not allowed in table name
            # Table name has to start with letter or '_'
            caseid_tbname = 'patient_' + f.caseid.replace('-', '_')

            # Create new table saving RNA-seq information for each patient
            if psql:
                # TODO: This step is slow
                database_connector.psql_saver(spark, df=match, tbname=caseid_tbname, savemode="overwrite")
            else:
                database_connector.redshift_saver(spark, df=match, tbname=caseid_tbname, \
                                                  tmpdir='rnaseq_%s' % f.caseid, savemode="overwrite")

            # Update the links to the saved RNA-seq table in patient_info table
            update_patient_info(caseid_tbname, f.caseid)

            cnt += 1
            if not cnt % 100:
                print('.', end=' ')

        except : #Py4JJavaError:
            print("Cannot process file: %s" % f.filepath)


if __name__ == "__main__":
    # Setup Driver for connection
    if psql:
        print("Using PostgreSQL as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./jars/postgresql-42.2.2.jar pyspark-shell'

        # Connect to PostgreSQL
        conn = psycopg2.connect(host=__credential__.host_psql, dbname=__credential__.dbname_psql,
                                user=__credential__.user_psql, password=__credential__.password_psql)
        cur = conn.cursor()

    else:
        print("Using Redshift as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./jars/spark-redshift_2.11-3.0.0-preview1.jar \
                            --jars ./jars/spark-avro_2.11-4.0.0.jar \
                            --jars ./jars/RedshiftJDBC41-1.2.12.1017.jar \
                            --jars ./jars/minimal-json-0.9.5.jar pyspark-shell'

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

    spark = SparkSession \
        .builder \
        .master(__credential__.spark_host) \
        .appName("txt_reader") \
        .getOrCreate()

    ref_genome = load_reference_genome(spark)
    process_rnaseq(spark, ref_genome)

    cur.close()
    conn.close()

    spark.stop()
