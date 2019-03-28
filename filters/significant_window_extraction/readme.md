Extract windows around significant positions
============================================

Scans summary statistic files and extracts only windows around significant regions.

This will massively reduce the size of the input data for fine-mapping and coloc pipelines.

Aim, keep rows based on proximity to other rows that meet these criteria:
- pval <= 0.05
- <= 10 bp away
- same study
- same chromosome
