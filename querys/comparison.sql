/*
Aggregate gene expression level across genome
between stage I and stage II of breast cancer in one table

sudo su postgres
psql --host='instance.userid.region.rds.amazonaws.com' --port='5432' --username='username' --dbname='dbname'

# TODO: use sd and mean to calculate p-value

*/

DROP TABLE IF EXISTS breast_stage2and1;
CREATE TABLE breast_stage2and1 AS (
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
                stage1.avg_expr AS stage1_avg_expr,
                stage2.avg_expr AS stage2_avg_expr
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