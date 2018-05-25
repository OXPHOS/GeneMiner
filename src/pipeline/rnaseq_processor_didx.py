# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract rna-seq information from tabular text files
and dump to PostgreSQL on Amazon RDS / Amazon Redshift

Table schema: case_id(i.e. patient_id, index), gene_id(index), gene_expr_val

Author: Pan Deng

"""

from os import environ, close
from pyspark.sql import SparkSession
from LocalConnector import LocalConnector
import database_connector
import __credential__
from io import BytesIO
from tempfile import mkstemp
from gzip import GzipFile
import csv
import boto3
import time

psql=True
test=False


def create_gene_expr_table():
    """
    Create double index table: Patient_id, Gene_id, Gene_expr
    """
    local_connector = LocalConnector(psql)
    conn, cur = local_connector.get_connection()
    cur.execute("""DROP TABLE IF EXISTS gene_expr_table""")
    cur.execute("""
        CREATE TABLE gene_expr_table (
            case_id text,
            gene_id text,
            gene_expr float
            )
        """)
    conn.commit()
    local_connector.close_connection()


def update_gene_expr_table(files):
    """
    Fill gene_expr_table with case_id (patient_id), gene_id and gene_expr_val
    
    :param files: Partitioned RDD containing filetype information
    """
    local_connector = LocalConnector(psql)
    conn, cur = local_connector.get_connection()
    s3 = boto3.client('s3', aws_access_key_id=__credential__.aws_access_key_id, \
                              aws_secret_access_key=__credential__.aws_secret_access_key)
    for f in files:
        try:  # TODO: import Error
            # Stream-in files from S3 and parse to list
            obj = s3.get_object(Bucket='gdcdata', Key=f.filepath)
            body = obj['Body'].read()
            content = GzipFile(None, 'r', fileobj=BytesIO(body)).read().decode('utf-8')
            content = list(csv.reader(content.split('\n'), delimiter='\t'))

            # Filter all genes with expression level == 0
            # Truncate the gene id from for eg. `ENSG00000007080.9` to `ENSG00000007080`
            # Convert to list: case_id, gene_id, expr_val
            gene_list = filter(lambda x: x[2] > 0, \
                               map(lambda x: (f.caseid, x[0].split('.')[0], float(x[1])), \
                                   filter(lambda x: len(x) > 1, content)))

            # Method 1
            # Write the list to temp csv file
            # Which is slow
            header = 'case_id\tgene_id\tgene_expr\n'
            fd, path = mkstemp(suffix='.csv')
            with open(path, 'w', newline='') as tf:
                tf.write(header)
                writer = csv.writer(tf, delimiter='\t')
                writer.writerows(gene_list)
            query = "COPY gene_expr_table FROM STDIN DELIMITER '\t' CSV HEADER"
            with open(path, 'r') as tf:
                cur.copy_expert(query, tf)
            conn.commit()
            close(fd)

            '''
            # Method 2
            # Insert by each row
            # Even slower
            import psycopg2
            from psycopg2 import extras
            query = """INSERT INTO gene_expr_table
                        VALUES (%s, %s, %s)"""
            psycopg2.extras.execute_batch(cur, query, gene_list)
            conn.commit()
            '''

        except:
            print("Unable to retrieve file: gdcdata/%s" % f.filepath)
            continue

    local_connector.close_connection()


def process_rnaseq(spark):
    """
    Retrieve txt files from pre-processed file list on psql

    :param spark: SparkSession
    """
    if psql:
        if test:
            filelist_rdd = database_connector.psql_file_loader(spark, tbname="txt_list")
        else:
            filelist_rdd = database_connector.psql_file_loader(spark, tbname="txt_list")
    else:
        filelist_rdd = database_connector.redshift_file_loader(spark, tbname="txt_list", tmpdir="txt_files")

    filelist_rdd\
        .filter(lambda x: '-UQ.txt' in x.filepath)\
        .repartition(18)\
        .foreachPartition(update_gene_expr_table)

if __name__ == "__main__":
    # Setup Driver for connection
    if psql:
        print("Using PostgreSQL as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./jars/postgresql-42.2.2.jar pyspark-shell'

    else:
        print("Using Redshift as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./jars/spark-redshift_2.11-3.0.0-preview1.jar \
                            --jars ./jars/spark-avro_2.11-4.0.0.jar \
                            --jars ./jars/RedshiftJDBC41-1.2.12.1017.jar \
                            --jars ./jars/minimal-json-0.9.5.jar pyspark-shell'

        print("Using Redshift as database.")
        environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./jars/spark-redshift_2.11-3.0.0-preview1.jar \
                            --jars ./jars/spark-avro_2.11-4.0.0.jar \
                            --jars ./jars/RedshiftJDBC41-1.2.12.1017.jar \
                            --jars ./jars/minimal-json-0.9.5.jar pyspark-shell'

    # Setup python path for worker nodes
    environ['PYSPARK_PYTHON'] = '/home/ubuntu/anaconda3/bin/python'
    environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/anaconda3/bin/jupyter'

    spark = SparkSession \
        .builder \
        .master(__credential__.spark_host) \
        .appName("txt_reader") \
        .getOrCreate()

    # IMPORTANT: to import module in the same python package
    spark.sparkContext.addPyFile('/home/ubuntu/GeneMiner/src/__credential__.py')
    spark.sparkContext.addPyFile('/home/ubuntu/GeneMiner/src/pipeline/LocalConnector.py')

    if test:
        start = time.time()
        create_gene_expr_table()
        process_rnaseq(spark)
        print("TOTAL TIME:", time.time()-start)
    else:
        create_gene_expr_table()
        process_rnaseq(spark)

    spark.stop()

    # Index the table
    # Better to be done in sql
    print("Indexing gene expression table...")
    local_connector = LocalConnector(psql)
    conn, cur = local_connector.get_connection()

    cur.execute("""CREATE INDEX cid ON gene_expr_table(case_id)""")
    cur.execute("""CREATE INDEX gid ON gene_expr_table(gene_id)""")
    conn.commit()

    local_connector.close_connection()
