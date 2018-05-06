# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract rna-seq information from tabular text files
and dump to Amazon Redshift

Author: Pan Deng

"""

from os import environ
from pyspark.sql import SparkSession, Row
import __credential__
import metainfo_loader


def load_reference_genome(spark):
    """
    Read reference genome data pre-stored on PostgreSQL to DataFrame
    
    :param spark: SparkSession
    :return: reference DataFrame 
    """
    ref_genome = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://%s' % __credential__.jdbc_accessible_host_psql) \
        .option('dbtable', __credential__.dbtable_psql) \
        .option('user', __credential__.user_psql) \
        .option('password', __credential__.password_psql) \
        .load()

    # Truncate the gene id from for eg. `ENSG00000007080.9` to `ENSG00000007080`
    ref_genome_trunc = ref_genome.rdd \
        .map(lambda x: Row(gene_id=x.id.split('.')[0], gene_name=x.name)) \
        .toDF()

    return ref_genome_trunc


def process_rnaseq(spark, ref):
    """
    Convert rnaseq FPKM-UQ data to readable tabular dataset with reference genome and dump to Redshift
    
    :param spark: SparkSession
    :param ref: reference genome with type DataFrame 
    """
    # Load clinical.xml file from Amazon S3
    filelist = metainfo_loader.file_loader(spark, "txt_list")
    cnt = 0
    print("Progress: every 100 processing")

    for f in filelist:
        # There are 3 types of RNAseq files representing similar results.
        # So only process -UQ files here
        if '-UQ.txt' not in f.filepath:
            continue
        # print(f.filepath, end=' ')

        # In tabular format of: [gene_id \t expression_value]
        fpkm = spark.read.format("csv") \
            .option("delimiter", "\t").option("quote", "") \
            .option("header", "false") \
            .option("inferSchema", "true") \
            .load("s3a://gdcdata/datasets/%s" % f.filepath)

        # Filter all genes with expression level = 0
        fpkm = fpkm.filter(fpkm._c1>0)

        # Truncate the gene id from for eg. `ENSG00000007080.9` to `ENSG00000007080`
        fpkm_trunc = fpkm.rdd \
            .map(lambda x: Row(gene_id=x[0].split('.')[0], expr_val=x[1])) \
            .toDF()

        cnt += 1
        if not cnt % 100:
            print('.', end=' ')

        # Join to reference genome to get table: gene_id | gene_name | expr_val
        match = fpkm_trunc.join(ref, 'gene_id')

        # print("Entries aligned: %s" % match.count(), end=' ')

        # TODO: Dump to database

if __name__ == "__main__":
    # Setup Driver for connection
    environ['PYSPARK_SUBMIT_ARGS'] = '--jars /usr/local/spark/jars/postgresql-42.2.2.jar pyspark-shell'

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

    spark.stop()