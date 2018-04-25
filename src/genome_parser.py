# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Convert annotated human genome sequence to table on PostgreSQL

Data source: https://useast.ensembl.org/info/data/ftp/index.html, EMBL

Author: Pan Deng

"""

import gzip
import os
import psycopg2
import re
import __credential__
from S3BatchUnzipper import S3BatchUnzipper

# path to annotated genome sequence
INPUT_PATH_LOCAL = os.path.abspath('../homo_sapiens/')
INPUT_PATH_AWS = ['gdcdata', 'homo_sapiens/']
DATA_FROM_S3 = True

# regex
PATTERNS = {'chromosome': '.*\.([a-z]+\.[0-9A-Z]+).dat.gz',  # The chromosome the gene is on.
            'gene_block': '  gene +(.*)',  # locate genomic sequence with strand and position information.
            'gene_strand': '.*(complement).*',  # The DNA strand the gene is on.
            'gene_position': '([0-9]+)\.\.([0-9]+)',  # start and end pos of a gene.
            'next_block': '  ([a-zA-Z_]+) ',  # Go to next block.
            'tag': '\/([a-z_]+)',  # To extract gene id, name or note.
            'id_info': '=([A-Z0-9.]+)',  # gene id.
            'name_info': '="(.+)"',  # gene name.
            'note_start': '.* +(\/note.+)\'', # gene note
            'note_continue': '.* +(.+)\''}  # wrapped note. without /note tag, switch_note should be on


class Gene:
    """
    Temporarily saves information of one gene
    """
    chromosome = 'Non-chromosomal'
    strand = None
    position = [-1, -1]
    id = None
    name = None
    info = ""
    other = ""

    def __repr__(self):
        return 'Gene\n\tid: %s \n\tname: %s \n\tchr: %s \n\tstrand: %s ' \
               '\n\tposition: %i..%i \n\tinfo: %s \n\tother: %s' \
                % (self.id, self.name, self.chromosome, self.strand,
                   self.position[0], self.position[1], self.info, self.other)


def create_table():
    """
    Refresh table
    """
    cur.execute("""
        DROP TABLE IF EXISTS hs_genome""")
    conn.commit()

    cur.execute("""
    CREATE TABLE hs_genome(
        id text PRIMARY KEY,
        name text,
        chromosome text,
        strand text,
        pos_start integer,
        pos_end integer,
        info text
    )
    """)
    conn.commit()


def count_table_rows():
    """
    :return: Count of rows in the table 
    """
    cur.execute("SELECT COUNT(*) FROM hs_genome")
    conn.commit()
    return cur.fetchone()[0]


def info_extractor(pattern, line, group_num=1, test=False):
    """
    Extract text in line that matches pattern and return the expected extracted text
    
    :param pattern: the regex pattern to be match
    :param line: the input text
    :param group_num: how many capturing groups are expected
    :param test: whether the function is called for test
    :return: extracted text
    """

    regxObj = re.compile(pattern, re.IGNORECASE | re.UNICODE)
    match = regxObj.search(line)

    if test:
        print('Line: ', line)
        print('Pattern: ', pattern)

    try:
        # Extract the capturing group
        if group_num == 1:
            extract = match.group(1)
        else:
            extract = [match.group(_) for _ in range(1, group_num+1)]
    # if no matching pattern is found
    except AttributeError:
        extract = ""
    return extract


def main(param):
    # Create PostgreSQL table
    create_table()

    # Aquire (.gz) file list from path
    if DATA_FROM_S3:
        try:
            assert len(param) == 2
            bucket, prefix = param
            unzipper = S3BatchUnzipper(bucket=bucket, targetdir=prefix)
            files = unzipper.get_gzip_file_list()
        except AssertionError:
            print("Bucket and Prefix information required for reading data from Amazon S3")
    else:
        try:
            assert os.path.exists(param)
            input_path = param
            files = [_ for _ in os.listdir(input_path) if _.endswith('dat.gz')]
            # files = ['Homo_sapiens.GRCh38.92.chromosome.14.dat.gz']  # for small scale test
        except AssertionError:
            print("Valid path to input files required")

    # Process all reference files
    for file in files:
        # Open file
        if DATA_FROM_S3:
            print("Reading genome file: %s" % file.name)
            f = S3BatchUnzipper.unzip_file(file.metadata)
            chr_num = info_extractor(PATTERNS['chromosome'], file.name)
        else:
            print("Reading genome file: %s" % file)
            f = gzip.open(os.path.join(input_path, file), 'r')
            chr_num = info_extractor(PATTERNS['chromosome'], file)

        cnt = 0  # for small scale test

        switch_reading = False  # Whether current line is in gene block
        switch_note = False  # Whether current line is describing notes of a gene
        gene = Gene()  # A new Gene

        for line in f:
            if switch_reading:
                # Current gene block ends
                if info_extractor(PATTERNS['next_block'], str(line)):
                    if switch_note:
                        # Clean up note information by removing '\note' tag and '"' in notes
                        try:
                            gene.info = gene.info.split("\"")[1]
                        except IndexError:
                            pass
                        switch_note = False
                    switch_reading = False

                    # Add the gene to database
                    # TODO: Bug on master node. Generate 2217 entries at local with chr.14 file but 2216 on masternode
                    # TODO: One of the 'gene'(ENSG00000276225.1)(possibly) gives error
                    try:
                        # print(gene)
                        cur.execute("INSERT INTO hs_genome VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                    (gene.id, gene.name, gene.chromosome, gene.strand,
                                       gene.position[0], gene.position[1], gene.info))
                        conn.commit()
                    except IndexError:
                        print('ERROR processing gene: %s' % gene.id)
                        # print(gene.id, gene.name, gene.chromosome, gene.position, gene.info)

                # Continue parsing current gene information
                else:
                    tag = info_extractor(PATTERNS['tag'], str(line))

                    if tag == 'gene':
                        gene.id = info_extractor(PATTERNS['id_info'], str(line))
                        switch_note = False
                    elif tag == 'locus_tag':
                        gene.name = info_extractor(PATTERNS['name_info'], str(line))
                        switch_note = False
                    elif switch_note:
                        gene.info += info_extractor(PATTERNS['note_continue'], str(line)).replace('\\n', ' ')
                    elif tag == 'note':
                        switch_note = True
                        gene.info += info_extractor(PATTERNS['note_start'], str(line)).replace('\\n', ' ')

            # If the new block is a gene, start a new Gene class
            gene_block = info_extractor(PATTERNS['gene_block'], str(line))
            if gene_block:
                switch_reading = True
                gene = Gene()

                # Mitochondrial DNA doesn't have strand information
                if 'MT' not in chr_num:
                    gene.strand = '-' if info_extractor(PATTERNS['gene_strand'], gene_block) else '+'

                # Non-chromosomal genes have disjoint gene positions, so ignored here.
                if chr_num:
                    gene.chromosome = chr_num
                    gene.position = info_extractor(PATTERNS['gene_position'], gene_block, 2)

                    # To avoid IndexError
                    if len(gene.position) == 2:
                        gene.position = list(map(int, gene.position))

            # For test purpose
            # cnt += 1
            # if cnt == 100:
            #    break
        # break
        print('Genes mapped: %i' % count_table_rows())

    # Total genes added to the database
    print("...............")
    print('Total genes mapped: %i' % count_table_rows())

if __name__ == "__main__":
    # TODO: arg parser for DATA_FROM_S3 flag, table name, and etc.
    if DATA_FROM_S3:
        print("- Data source: Amazon AWS")
        input_param = INPUT_PATH_AWS
    else:
        print("- Data source: local machine")
        input_param = INPUT_PATH_LOCAL

    # Connect to PostSQL with the credentials
    # Setup the connector and cursor of the database
    conn = psycopg2.connect(host=__credential__.host, dbname=__credential__.dbname,
                            user=__credential__.user, password=__credential__.password)
    cur = conn.cursor()

    main(input_param)
