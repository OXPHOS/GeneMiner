/*
Aggregate gene expression level across genome
between stage I and stage II of breast cancer in one table

sudo su postgres
psql --host='instance.userid.region.rds.amazonaws.com' --port='5432' --username='username' --dbname='dbname'

# TODO: use sd and mean to calculate p-value

*/

/* BREAST */
DROP TABLE IF EXISTS exprcmp_breast;
CREATE TABLE exprcmp_breast AS (
    WITH stage2and1 AS (
            WITH stage1 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Breast'
                                AND (disease_stage = 'Stage I' OR
                                    disease_stage = 'Stage IA' OR
                                    disease_stage = 'Stage IB'
                                )
                        )
                    GROUP BY gene_id
                ), stage2 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Breast'
                                AND (disease_stage = 'Stage II' OR
                                    disease_stage = 'Stage IIA' OR
                                    disease_stage = 'Stage IIB'
                                )
                        )
                    GROUP BY gene_id
                )
            SELECT stage1.gene_id,
                stage1.avg_expr AS early_avg_expr,
                stage2.avg_expr AS late_avg_expr
            FROM stage1 INNER JOIN stage2 ON (stage1.gene_id = stage2.gene_id)
            WHERE (stage2.avg_expr / stage1.avg_expr > 2.0) OR
                   (stage2.avg_expr / stage1.avg_expr < 0.5)
        )
    SELECT stage2and1.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage2and1 LEFT OUTER JOIN hs_genome ON (stage2and1.gene_id = hs_genome.id)
);

/*
7528 in total (versus 60000+ total)
*/


/* Bladder */
DROP TABLE IF EXISTS exprcmp_bladder;
CREATE TABLE exprcmp_bladder AS (
    WITH stage3and2 AS (
            WITH stage2 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Bladder'
                                AND disease_stage = 'Stage II'
                        )
                    GROUP BY gene_id
                ), stage3 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Bladder'
                                AND disease_stage = 'Stage III'
                        )
                    GROUP BY gene_id
                )
            SELECT stage3.gene_id,
                stage2.avg_expr AS early_avg_expr,
                stage3.avg_expr AS late_avg_expr
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
            WHERE (stage3.avg_expr / stage2.avg_expr > 2.0) OR
                   (stage3.avg_expr / stage2.avg_expr < 0.5)
        )
    SELECT stage3and2.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage3and2 LEFT OUTER JOIN hs_genome ON (stage3and2.gene_id = hs_genome.id)
);

/* Colon */
DROP TABLE IF EXISTS exprcmp_colon;
CREATE TABLE exprcmp_colon AS (
    WITH stage3and2 AS (
            WITH stage2 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Colon'
                                AND disease_stage ~ 'Stage II'
                        )
                    GROUP BY gene_id
                ), stage3 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Colon'
                                AND disease_stage ~ 'Stage III'
                        )
                    GROUP BY gene_id
                )
            SELECT stage3.gene_id,
                stage3.avg_expr AS late_avg_expr,
                stage2.avg_expr AS early_avg_expr
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
            WHERE (stage3.avg_expr / stage2.avg_expr > 2.0) OR
                   (stage3.avg_expr / stage2.avg_expr < 0.5)
        )
    SELECT stage3and2.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage3and2 LEFT OUTER JOIN hs_genome ON (stage3and2.gene_id = hs_genome.id)
);


/* Esophagus */
DROP TABLE IF EXISTS exprcmp_esophagus;
CREATE TABLE exprcmp_esophagus AS (
    WITH stage2band2a AS (
            WITH stage2b AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Esophagus'
                                AND disease_stage = 'Stage IIB'
                        )
                    GROUP BY gene_id
                ), stage2a AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Esophagus'
                                AND disease_stage = 'Stage IIA'
                        )
                    GROUP BY gene_id
                )
            SELECT stage2b.gene_id,
                stage2b.avg_expr AS late_avg_expr,
                stage2a.avg_expr AS early_avg_expr
            FROM stage2b INNER JOIN stage2a ON (stage2b.gene_id = stage2a.gene_id)
            WHERE (stage2b.avg_expr / stage2a.avg_expr > 2.0) OR
                   (stage2b.avg_expr / stage2a.avg_expr < 0.5)
        )
    SELECT stage2band2a.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage2band2a LEFT OUTER JOIN hs_genome ON (stage2band2a.gene_id = hs_genome.id)
);


/* Extremities */
DROP TABLE IF EXISTS exprcmp_extremities;
CREATE TABLE exprcmp_extremities(
    gene_id text,
    late_avg_expr float,
    early_avg_expr float,
    gene_name text
);


/* Head and Neck */
DROP TABLE IF EXISTS exprcmp_headandneck;
CREATE TABLE exprcmp_headandneck AS (
    WITH stage3and2 AS (
            WITH stage2 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Head and Neck'
                                AND disease_stage = 'Stage II'
                        )
                    GROUP BY gene_id
                ), stage3 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Head and Neck'
                                AND disease_stage = 'Stage III'
                        )
                    GROUP BY gene_id
                )
            SELECT stage3.gene_id,
                stage3.avg_expr AS late_avg_expr,
                stage2.avg_expr AS early_avg_expr
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
            WHERE (stage3.avg_expr / stage2.avg_expr > 2.0) OR
                   (stage3.avg_expr / stage2.avg_expr < 0.5)
        )
    SELECT stage3and2.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage3and2 LEFT OUTER JOIN hs_genome ON (stage3and2.gene_id = hs_genome.id)
);


/* Kidney */
DROP TABLE IF EXISTS exprcmp_Kidney;
CREATE TABLE exprcmp_Kidney AS (
    WITH stage3and2 AS (
            WITH stage2 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Kidney'
                                AND disease_stage = 'Stage II'
                        )
                    GROUP BY gene_id
                ), stage3 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Kidney'
                                AND disease_stage = 'Stage III'
                        )
                    GROUP BY gene_id
                )
            SELECT stage3.gene_id,
                stage3.avg_expr AS late_avg_expr,
                stage2.avg_expr AS early_avg_expr
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
            WHERE (stage3.avg_expr / stage2.avg_expr > 2.0) OR
                   (stage3.avg_expr / stage2.avg_expr < 0.5)
        )
    SELECT stage3and2.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage3and2 LEFT OUTER JOIN hs_genome ON (stage3and2.gene_id = hs_genome.id)
);


/* Liver */
DROP TABLE IF EXISTS exprcmp_Liver;
CREATE TABLE exprcmp_Liver AS (
    WITH stage2and1 AS (
            WITH stage2 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Liver'
                                AND disease_stage = 'Stage II'
                        )
                    GROUP BY gene_id
                ), stage1 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Liver'
                                AND disease_stage = 'Stage I'
                        )
                    GROUP BY gene_id
                )
            SELECT stage2.gene_id,
                stage1.avg_expr AS early_avg_expr,
                stage2.avg_expr AS late_avg_expr
            FROM stage1 INNER JOIN stage2 ON (stage1.gene_id = stage2.gene_id)
            WHERE (stage2.avg_expr / stage1.avg_expr > 2.0) OR
                   (stage2.avg_expr / stage1.avg_expr < 0.5)
        )
    SELECT stage2and1.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage2and1 LEFT OUTER JOIN hs_genome ON (stage2and1.gene_id = hs_genome.id)
);


/* Lung */
DROP TABLE IF EXISTS exprcmp_Lung;
CREATE TABLE exprcmp_Lung AS (
    WITH stage1band1a AS (
            WITH stage1a AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Lung'
                                AND disease_stage = 'Stage IA'
                        )
                    GROUP BY gene_id
                ), stage1b AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Lung'
                                AND disease_stage = 'Stage IB'
                        )
                    GROUP BY gene_id
                )
            SELECT stage1b.gene_id,
                stage1b.avg_expr AS late_avg_expr,
                stage1a.avg_expr AS early_avg_expr
            FROM stage1b INNER JOIN stage1a ON (stage1b.gene_id = stage1a.gene_id)
            WHERE (stage1b.avg_expr / stage1a.avg_expr > 2.0) OR
                   (stage1b.avg_expr / stage1a.avg_expr < 0.5)
        )
    SELECT stage1band1a.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage1band1a LEFT OUTER JOIN hs_genome ON (stage1band1a.gene_id = hs_genome.id)
);


/* Other  Specify */
DROP TABLE IF EXISTS exprcmp_OtherSpecify;
CREATE TABLE exprcmp_OtherSpecify(
    gene_id text,
    late_avg_expr float,
    early_avg_expr float,
    gene_name text
);


/* Pancreas */
DROP TABLE IF EXISTS exprcmp_Pancreas;
CREATE TABLE exprcmp_Pancreas AS (
    WITH stage2band2a AS (
            WITH stage2a AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Pancreas'
                                AND disease_stage = 'Stage IIA'
                        )
                    GROUP BY gene_id
                ), stage2b AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Pancreas'
                                AND disease_stage = 'Stage IIB'
                        )
                    GROUP BY gene_id
                )
            SELECT stage2b.gene_id,
                stage2b.avg_expr AS late_avg_expr,
                stage2a.avg_expr AS early_avg_expr
            FROM stage2b INNER JOIN stage2a ON (stage2b.gene_id = stage2a.gene_id)
            WHERE (stage2b.avg_expr / stage2a.avg_expr > 2.0) OR
                   (stage2b.avg_expr / stage2a.avg_expr < 0.5)
        )
    SELECT stage2band2a.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage2band2a LEFT OUTER JOIN hs_genome ON (stage2band2a.gene_id = hs_genome.id)
);


/* Rectum */
DROP TABLE IF EXISTS exprcmp_Rectum;
CREATE TABLE exprcmp_Rectum(
    gene_id text,
    late_avg_expr float,
    early_avg_expr float,
    gene_name text
);


/* Stomach */
DROP TABLE IF EXISTS exprcmp_Stomach;
CREATE TABLE exprcmp_Stomach AS (
    WITH stage3band3a AS (
            WITH stage3a AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Stomach'
                                AND disease_stage = 'Stage IIIA'
                        )
                    GROUP BY gene_id
                ), stage3b AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Stomach'
                                AND disease_stage = 'Stage IIIB'
                        )
                    GROUP BY gene_id
                )
            SELECT stage3b.gene_id,
                stage3b.avg_expr AS late_avg_expr,
                stage3a.avg_expr AS early_avg_expr
            FROM stage3b INNER JOIN stage3a ON (stage3b.gene_id = stage3a.gene_id)
            WHERE (stage3b.avg_expr / stage3a.avg_expr > 3.0) OR
                   (stage3b.avg_expr / stage3a.avg_expr < 0.5)
        )
    SELECT stage3band3a.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage3band3a LEFT OUTER JOIN hs_genome ON (stage3band3a.gene_id = hs_genome.id)
);


/* Thyroid */
DROP TABLE IF EXISTS exprcmp_Thyroid;
CREATE TABLE exprcmp_Thyroid AS (
    WITH stage3and2 AS (
            WITH stage2 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Thyroid'
                                AND disease_stage = 'Stage II'
                        )
                    GROUP BY gene_id
                ), stage3 AS (
                    SELECT gene_id, AVG(gene_expr) as avg_expr
                    FROM gene_expr_table
                    WHERE case_id IN (
                        SELECT case_id
                            FROM patient_info
                            WHERE disease_type = 'Thyroid'
                                AND disease_stage = 'Stage III'
                        )
                    GROUP BY gene_id
                )
            SELECT stage3.gene_id,
                stage3.avg_expr AS late_avg_expr,
                stage2.avg_expr AS early_avg_expr
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
            WHERE (stage3.avg_expr / stage2.avg_expr > 2.0) OR
                   (stage3.avg_expr / stage2.avg_expr < 0.5)
        )
    SELECT stage3and2.*, hs_genome.name as gene_name, hs_genome.chromosome, hs_genome.info
    FROM stage3and2 LEFT OUTER JOIN hs_genome ON (stage3and2.gene_id = hs_genome.id)
);


/* Trunk */
DROP TABLE IF EXISTS exprcmp_trunk;
CREATE TABLE exprcmp_trunk(
    gene_id text,
    late_avg_expr float,
    early_avg_expr float,
    gene_name text
);