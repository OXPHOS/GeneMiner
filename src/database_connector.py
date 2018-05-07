# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract pre-stored file information

Author: Pan Deng

"""

from pyspark.sql import SparkSession, Row
from os import environ
import __credential__


def psql_saver(spark, df, tbname, savemode='error'):
    """
    Save DataFrame to PostgreSQL via JDBC postgresql driver
    
    :param spark: SparkSession
    :param df: dataframe to be saved
    :param tbname: table name
    :param savemode: error: report error if exists
                     overwrite: overwrite the table if exists
                     append: append the the table if exists
                     ignore: no updates if the table exists
    """
    df.createOrReplaceTempView("view")
    spark.sql('''SELECT * FROM view''').write \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://%s' % __credential__.jdbc_accessible_host_psql) \
        .option('dbtable', tbname) \
        .option('user', __credential__.user_psql) \
        .option('password', __credential__.password_psql) \
        .mode(savemode) \
        .save()


def psql_loader(spark, tbname):
    """
    Load table from PostgreSQL and return DataFrame

    :param spark: SparkSession
    :param tbname: The table to be loaded
    :return: DataFrame loaded from PostgreSQL table
    """
    print("Loading files from PostgreSQL table: %s" % tbname)
    filelist = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://%s' % __credential__.jdbc_accessible_host_psql) \
        .option('dbtable', tbname) \
        .option('user', __credential__.user_psql) \
        .option('password', __credential__.password_psql) \
        .load()

    return filelist


def psql_file_loader(spark, tbname):
    """
    Load the file list from PostgreSQL and return the readable filepath.

    :param spark: SparkSession
    :param tbname: The table saving file information 
    :return: List of files
    """
    filelist_rdd = psql_loader(spark, tbname) \
        .rdd.map(lambda x: Row(caseid=x.case_id, filepath=x.path + '/' + x.filename))
    return list(filelist_rdd.collect())


def redshift_saver(spark, df, tbname, tmpdir, savemode='error'):
    """
    Save DataFrame to Redshift via JDBC redshift driver
    
    :param spark: SparkSession
    :param df: dataframe to be saved
    :param tbname: table name
    :param tmpdir: tmp S3 dir to store data
    :param savemode: error: report error if exists
                     overwrite: overwrite the table if exists
                     append: append the the table if exists
                     ignore: no updates if the table exists
    """
    df.createOrReplaceTempView("view")
    spark.sql('''SELECT * FROM view''') \
        .write.format("com.databricks.spark.redshift") \
        .option("url", __credential__.jdbc_accessible_host_redshift) \
        .option("dbtable", tbname) \
        .option("forward_spark_s3_credentials", True) \
        .option("tempdir", "s3n://gdcdata/%s" % tmpdir) \
        .mode(savemode) \
        .save()


def redshift_loader(spark, tbname, tmpdir):
    """
     Load table from Redshift and return DataFrame

    :param spark: SparkSession
    :param tbname: The table to be loaded
    :param tmpdir: tmp S3 dir to store data
    :return: DataFrame load from Redshift table
    """
    print("Loading files from Redshift table: %s" % tbname)
    filelist = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", __credential__.jdbc_accessible_host_redshift) \
        .option("forward_spark_s3_credentials", True) \
        .option("dbtable", tbname) \
        .option("tempdir", "s3n://gdcdata/%s" % tmpdir) \
        .load()
    return filelist


def redshift_file_loader(spark, tbname, tmpdir):
    """
    Load the file list from Redshift and return the readable filepath.

    :param spark: SparkSession
    :param tbname: The table saving file information 
    :param tmpdir: tmp S3 dir to store data
    :return: List of files
    """
    filelist_rdd = redshift_loader(spark, tbname, tmpdir) \
        .rdd.map(lambda x: Row(caseid=x.case_id, filepath=x.path + '/' + x.filename))
    return list(filelist_rdd.collect())


if __name__ == "__main__":
    # Setup Driver for connection
    # NOTE: PostgreSQL driver and RedShift driver are not compatible.
    # Loading Redshift driver will cause errors connecting to PostgreSQL databse
    # Didn't look into solutions

    # environ['PYSPARK_SUBMIT_ARGS'] = '--jars /usr/local/spark/jars/postgresql-42.2.2.jar pyspark-shell'

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
        .appName("meta_info_loador") \
        .getOrCreate()

    # redshift_loader(spark)

    spark.stop()
