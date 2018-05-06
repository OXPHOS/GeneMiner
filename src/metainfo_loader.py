# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Extract pre-stored file information

Author: Pan Deng

"""

from pyspark.sql import SparkSession, Row
import __credential__
from os import environ


def file_loader(spark, tbname):
    """
    Load the file list from PostgreSQL and return the readable filepath.
    
    :param spark: SparkSession
    :param tbname: The table saving file information 
    :return: 
    """
    print("Loading files from PostgreSQL table: %s" % tbname)
    filelist = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://%s' % __credential__.jdbc_accessible_host_psql) \
        .option('dbtable', tbname) \
        .option('user', __credential__.user_psql) \
        .option('password', __credential__.password_psql) \
        .load()

    filelist_rdd = filelist.rdd.map(lambda x: Row(filepath=x.path + '/' + x.filename))
    return list(filelist_rdd.collect())


if __name__ == "__main__":
    # Setup Driver for connection
    environ['PYSPARK_SUBMIT_ARGS'] = '--jars /usr/local/spark/jars/postgresql-42.2.2.jar pyspark-shell'

    # Setup python path for worker nodes
    environ['PYSPARK_PYTHON'] = '/home/ubuntu/anaconda3/bin/python'
    environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/anaconda3/bin/jupyter'

    spark = SparkSession \
        .builder \
        .master(__credential__.spark_host) \
        .appName("meta_info_loador") \
        .getOrCreate()

    file_loader(spark)

    spark.stop()
