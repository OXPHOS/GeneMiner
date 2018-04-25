"""
Convert annotated human genome sequence to table in PostgreSQL

Data source: https://useast.ensembl.org/info/data/ftp/index.html, EMBL
"""

import gzip
import os
import psycopg2
import re
import __credential__


# path to annotated genome sequence
INPUT_PATH_LOCAL = os.path.abspath('../homo_sapiens/')

# regex
PATTERNS = {'chromosome': '.*\.([a-z]+\.[0-9A-Z]+).dat.gz',  # The chromosome the gene is on.
            'gene_block': '  gene +(.*)',  # locate genomic sequence with strand and position information.
            'gene_strand': '.*(complement).*',  # The DNA strand the gene is on.
            'gene_position': '([0-9]+)\.\.([0-9]+)',  # start and end pos of a gene.
            'next_block': '  ([a-zA-Z_]+) ',  # Go to next block.
            'tag': '\/([a-z_]+)',  # To extract gene id, name or note.
            'id_info': '=([A-Z0-9.]+)',  # gene id.
            'name_info': '="(.+)"',  # gene name.
            'note_info': ' {21}(.+)\''}  # gene note.


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
    Connect to PostgreSQL server via credentials and refresh table
    
    :return: connector and cursor of the database
    """
    conn = psycopg2.connect(host=__credential__.host, dbname=__credential__.dbname,
                            user=__credential__.user, password=__credential__.password)
    cur = conn.cursor()

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

    return conn, cur


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


def main(path):
    input_path = path

    # Create PostgreSQL table
    conn, cur = create_table()

    files = [_ for _ in os.listdir(input_path) if _.endswith('dat.gz')]
    # files = ['Homo_sapiens.GRCh38.92.chromosome.14.dat.gz']  # for small scale test

    for file in files:
        print("Reading genome file: %s" %file)
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
                        gene.info = gene.info.split("\"")[1]
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
                    elif tag == 'note' or switch_note:
                        switch_note = True
                        gene.info += info_extractor(PATTERNS['note_info'], str(line)).replace('\\n', ' ')

            # If the new block is a gene, start a new Gene class
            gene_block = info_extractor(PATTERNS['gene_block'], str(line))
            if gene_block:
                switch_reading = True
                gene = Gene()

                if 'MT' not in chr_num:  # Mitochondrial DNA doesn't have strand information
                    gene.strand = '-' if info_extractor(PATTERNS['gene_strand'], gene_block) else '+'

                if chr_num:  # Non-chromosomal genes have disjoint gene positions, so ignored here.
                    gene.chromosome = chr_num
                    gene.position = info_extractor(PATTERNS['gene_position'], gene_block, 2)

                    # To avoid IndexError
                    if len(gene.position) == 2:
                        gene.position = list(map(int, gene.position))

            # For test purpose
            # cnt += 1
            # if cnt == 2000:
            #     break


if __name__ == "__main__":
    input_path = INPUT_PATH_LOCAL

    main(input_path)
