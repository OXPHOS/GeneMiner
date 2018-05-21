ETL pipeline:


![pipeline](https://github.com/OXPHOS/GeneMiner/blob/master/images/pipeline.png)
 

 - [`metainfo_processor`](https://github.com/OXPHOS/GeneMiner/blob/master/src/pipeline/metainfo_processor.py):
Integrate metadata JSON file and manifest file, and export to PostgreSQL

 - [`xml_processor`](https://github.com/OXPHOS/GeneMiner/blob/master/src/pipeline/xml_processor.py): 
Extract disease type, disease stage and patient gender from xml files
and dump to PostgreSQL on Amazon RDS 
 
 - [`rnaseq_processor_didx`](https://github.com/OXPHOS/GeneMiner/blob/master/src/pipeline/rnaseq_processor_didx.py):
Extract rna-seq information from tabular text files
and dump to PostgreSQL on Amazon RDS

 - [`database_connector`](https://github.com/OXPHOS/GeneMiner/blob/master/src/pipeline/database_connector.py)
 Load from and save files to PostgreSQL or Redshift with jdbc drivers
 
 - [`LocalConnector`](https://github.com/OXPHOS/GeneMiner/blob/master/src/pipeline/LocalConnector.py)
 Setup connector to PostgreSQL or Redshift with psycopg2
 