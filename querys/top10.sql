/*
Compare gene expression level across genome
between stage I and stage II of breast cancer

sudo su postgres
psql --host='instance.userid.region.rds.amazonaws.com' --port='5432' --username='username' --dbname='dbname'

*/

DROP TABLE IF EXISTS refed_stage2to1;
CREATE TABLE breast_stage2to1 AS (
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