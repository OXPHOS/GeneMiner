/*
Compare gene expression level across genome
between stage I and stage II of breast cancer

sudo su postgres
psql --host='instance.userid.region.rds.amazonaws.com' --port='5432' --username='username' --dbname='dbname'

*/

DROP TABLE IF EXISTS exprsort_breast;
CREATE TABLE exprsort_breast AS (
    WITH stage2to1 AS (
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
            SELECT stage1.gene_id, (stage2.avg_expr / stage1.avg_expr) AS fold_change
            FROM stage1 INNER JOIN stage2 ON (stage1.gene_id = stage2.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage2to1 LEFT OUTER JOIN hs_genome ON (stage2to1.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Bladder */
DROP TABLE IF EXISTS exprsort_bladder;
CREATE TABLE exprsort_bladder AS (
    WITH stage3to2 AS (
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
            SELECT stage3.gene_id, (stage3.avg_expr / stage2.avg_expr) AS fold_change
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage3to2 LEFT OUTER JOIN hs_genome ON (stage3to2.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Colon */
DROP TABLE IF EXISTS exprsort_colon;
CREATE TABLE exprsort_colon AS (
    WITH stage3to2 AS (
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
            SELECT stage3.gene_id, (stage3.avg_expr / stage2.avg_expr) AS fold_change
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage3to2 LEFT OUTER JOIN hs_genome ON (stage3to2.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Esophagus */
DROP TABLE IF EXISTS exprsort_esophagus;
CREATE TABLE exprsort_esophagus AS (
    WITH stage2bto2a AS (
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
            SELECT stage2b.gene_id, (stage2b.avg_expr / stage2a.avg_expr) AS fold_change
            FROM stage2b INNER JOIN stage2a ON (stage2b.gene_id = stage2a.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage2bto2a LEFT OUTER JOIN hs_genome ON (stage2bto2a.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Extremities */
DROP TABLE IF EXISTS exprsort_extremities;
CREATE TABLE exprsort_extremities(
    gene_name text,
    info text,
    fold_change float
);


/* Head and Neck */
DROP TABLE IF EXISTS exprsort_headandneck;
CREATE TABLE exprsort_headandneck AS (
    WITH stage3to2 AS (
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
            SELECT stage3.gene_id, (stage3.avg_expr / stage2.avg_expr) AS fold_change
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage3to2 LEFT OUTER JOIN hs_genome ON (stage3to2.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Kidney */
DROP TABLE IF EXISTS exprsort_Kidney;
CREATE TABLE exprsort_Kidney AS (
    WITH stage3to2 AS (
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
            SELECT stage3.gene_id, (stage3.avg_expr / stage2.avg_expr) AS fold_change
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage3to2 LEFT OUTER JOIN hs_genome ON (stage3to2.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Liver */
DROP TABLE IF EXISTS exprsort_Liver;
CREATE TABLE exprsort_Liver AS (
    WITH stage2to1 AS (
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
            SELECT stage1.gene_id, (stage2.avg_expr / stage1.avg_expr) AS fold_change
            FROM stage1 INNER JOIN stage2 ON (stage1.gene_id = stage2.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage2to1 LEFT OUTER JOIN hs_genome ON (stage2to1.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Lung */
DROP TABLE IF EXISTS exprsort_Lung;
CREATE TABLE exprsort_Lung AS (
    WITH stage1bto1a AS (
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
            SELECT stage1a.gene_id, (stage1b.avg_expr / stage1a.avg_expr) AS fold_change
            FROM stage1b INNER JOIN stage1a ON (stage1b.gene_id = stage1a.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage1bto1a LEFT OUTER JOIN hs_genome ON (stage1bto1a.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Other  Specify */
DROP TABLE IF EXISTS exprsort_OtherSpecify;
CREATE TABLE exprsort_OtherSpecify(
    gene_name text,
    info text,
    fold_change float
);


/* Pancreas */
DROP TABLE IF EXISTS exprsort_Pancreas;
CREATE TABLE exprsort_Pancreas AS (
    WITH stage2bto2a AS (
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
            SELECT stage2a.gene_id, (stage2b.avg_expr / stage2a.avg_expr) AS fold_change
            FROM stage2b INNER JOIN stage2a ON (stage2b.gene_id = stage2a.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage2bto2a LEFT OUTER JOIN hs_genome ON (stage2bto2a.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Rectum */
DROP TABLE IF EXISTS exprsort_Rectum;
CREATE TABLE exprsort_Rectum(
    gene_name text,
    info text,
    fold_change float
);


/* Stomach */
DROP TABLE IF EXISTS exprsort_Stomach;
CREATE TABLE exprsort_Stomach AS (
    WITH stage3bto3a AS (
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
            SELECT stage3b.gene_id, (stage3b.avg_expr / stage3a.avg_expr) AS fold_change
            FROM stage3a INNER JOIN stage3b ON (stage3a.gene_id = stage3b.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage3bto3a LEFT OUTER JOIN hs_genome ON (stage3bto3a.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Thyroid */
DROP TABLE IF EXISTS exprsort_Thyroid;
CREATE TABLE exprsort_Thyroid AS (
    WITH stage3to2 AS (
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
           SELECT stage3.gene_id, (stage3.avg_expr / stage2.avg_expr) AS fold_change
            FROM stage3 INNER JOIN stage2 ON (stage3.gene_id = stage2.gene_id)
        )
    SELECT gene_id, hs_genome.name as gene_name, fold_change, hs_genome.chromosome, hs_genome.info
    FROM stage3to2 LEFT OUTER JOIN hs_genome ON (stage3to2.gene_id = hs_genome.id)
    ORDER BY fold_change DESC
);


/* Trunk */
DROP TABLE IF EXISTS exprsort_trunk;
CREATE TABLE exprsort_trunk(
    gene_name text,
    info text,
    fold_change float
);
