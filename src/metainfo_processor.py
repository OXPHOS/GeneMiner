from pyspark.sql import SparkSession
import __credential__
from os import environ


def psql_saver(df, tbname, savemode='error'):
    df.createOrReplaceTempView("view")
    spark.sql('''SELECT * FROM view''').write \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://%s' % __credential__.jdbc_accessible_host_psql) \
        .option('dbtable', tbname) \
        .option('user', __credential__.user_psql) \
        .option('password', __credential__.password_psql) \
        .mode(savemode) \
        .save()


def main():
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
            SELECT manifest_view.filename, manifest_view.path, meta_view.data_format, \
            meta_view.cases.project.project_id, meta_view.cases.case_id 
            FROM manifest_view
            INNER JOIN meta_view ON  manifest_view.filename=meta_view.file_name
            ''')
    index.createOrReplaceTempView("index_view")

    # Split files and save to PostgreSQL
    TableByFormat = {'BCR XML': 'xml_list', 'TXT': 'txt_list'}

    # Group files by column: data_format
    files_groupby_types = list(map(
        lambda key: {'type': key,'flist': index.filter(index.data_format == key)},
        TableByFormat))
    for files in files_groupby_types:
        print("Saving [%s] data to PostgreSQL table [%s]..." \
              % (files['type'], TableByFormat[files['type']]))
        psql_saver(files['flist'], TableByFormat[files['type']], 'overwrite')

    # Save unreadable files
    unreadable = index.rdd.filter(lambda x: x.data_format not in TableByFormat)
    if unreadable.count():
        print("Saving data in unkown foramt to PostgreSQL table: unknowns.")
        psql_saver(unreadable.toDF(), 'unknowns', 'overwrite')


if __name__ == "__main__":
    # Setup Driver for connection
    environ['PYSPARK_SUBMIT_ARGS'] = '--jars /usr/local/spark/jars/postgresql-42.2.2.jar pyspark-shell'

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