Raw annotated genome sequence downloaded from EMBL:

https://useast.ensembl.org/info/data/ftp/index.html

Parse and save the downloaded data (in S3 or at local) to PostgreSQL on Amazon RDS with Python.
 

 - [`genome_parser`](https://github.com/OXPHOS/GeneMiner/blob/master/src/ref_genome/genome_parser.py):
 Convert annotated human genome sequence to table on PostgreSQL

 - [`info_extractor`](https://github.com/OXPHOS/GeneMiner/blob/master/src/ref_genome/info_extractor.py): 
 Capture regex pattern in given text
 
 - [`S3BatchUnzipper`](https://github.com/OXPHOS/GeneMiner/blob/master/src/ref_genome/S3BatchUnzipper.py):
 Unzip .gz files stored at Amazon S3 with `Boto3`
 
 