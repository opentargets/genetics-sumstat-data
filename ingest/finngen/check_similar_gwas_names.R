suppressMessages(library(tidyverse))
library(jsonlite)
theme_set(theme_bw())

# In FinnGen there are many endpoints that are very similar. Most of these
# relate to cancers, e.g.:
# CD2_BENIGN_APPENDIX
# CD2_BENIGN_APPENDIX_EXALLC
# The first is a GWAS of benign appendix neoplasm, and the second is a GWAS
# for the same trait but where control samples having any cancer are excluded.
#
# This script is to check the number of associations for these similar GWAS.

r5_gwas = jsonlite::stream_in(file("/Users/jeremys/work/otgenetics/genetics-sumstat-data/ingest/finngen/configs/inputs/r5_finngen.json"))

r5_cancer = r5_gwas %>%
  filter(grepl("^CD2_", phenocode)) %>%
  mutate(excluding_cancer_controls = grepl("EXALLC", phenocode))


ggplot(r5_cancer, aes(x=num_gw_significant, color=excluding_cancer_controls)) +
  geom_histogram() +
  facet_wrap(~excluding_cancer_controls, ncol = 1) +
  scale_x_log10()

print(sprintf("Number of loci original: %d", sum(r5_cancer %>% filter(!excluding_cancer_controls) %>% .$num_gw_significant)))
print(sprintf("Number of loci (excl cancer controls): %d", sum(r5_cancer %>% filter(excluding_cancer_controls) %>% .$num_gw_significant)))

# You could make an interesting plot showing how the number of loci changes
# for each GWAS, i.e. a line plot with a line for each GWAS, on the left
# showing the number with all controls, and on the right the number with
# more controls excluded. But it's not worth it at this point.
