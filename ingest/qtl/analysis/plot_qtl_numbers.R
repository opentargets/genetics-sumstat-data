library(tidyverse)
theme_set(theme_bw())

args = commandArgs(trailingOnly=TRUE)
qvals_file = args[1]
output_root = args[2]

df = readr::read_csv(qvals_file)

df = df %>%
  mutate(dataset = paste(study_id, bio_feature, sep='-'))

genes_tested = df %>%
  group_by(dataset) %>%
  summarise(num_genes_tested = n())

df_list = list()
thresholds = data.frame(name = c("p < 1e-4", "p < 1e-5", "p < 1e-6", "p < 5e-8"),
                        threshold = c(1e-4, 1e-5, 1e-6, 5e-8))
for (i in 1:nrow(thresholds)) {
  qtl_count = df  %>%
    group_by(dataset) %>%
    summarise(count = sum(min_pval < thresholds$threshold[i]))
  qtl_count$name = thresholds$name[i]
  df_list = c(list(qtl_count), df_list)
}

qtl_count_fdr5 = df  %>%
  group_by(dataset) %>%
  summarise(count = sum(qval < 0.05))
qtl_count_fdr5$name = "FDR 5%"

qtl_count_pbonf5 = df  %>%
  group_by(dataset) %>%
  summarise(count = sum(p_bonf < 0.05))
qtl_count_pbonf5$name = "pbonf < 0.05"
df_list = c(list(qtl_count_pbonf5), list(qtl_count_fdr5), df_list)

qtl_counts = bind_rows(df_list)

qtl_counts_at_gws = qtl_counts %>%
  filter(name == "p < 5e-8") %>%
  rename(gws_count = count) %>%
  select(dataset, gws_count)
qtl_counts = qtl_counts %>%
  left_join(qtl_counts_at_gws, by="dataset") %>%
  rename(threshold = name)
qtl_counts$threshold = factor(qtl_counts$threshold, levels=c("p < 5e-8", "p < 1e-6", "FDR 5%", "pbonf < 0.05", "p < 1e-5", "p < 1e-4"))

# ggplot(qtl_counts, aes(x=fct_reorder(dataset, gws_count), y=count, color=threshold, group=threshold)) +
#   geom_line() +
#   theme(axis.text.y = element_text(size=6)) +
#   coord_flip()

qtl_counts_flt = qtl_counts %>%
  filter(threshold != "p < 1e-4",
         dataset != "eQTLGen-UBERON_0000178")
p = ggplot(qtl_counts_flt, aes(x=fct_reorder(dataset, gws_count), y=count, color=threshold, group=threshold)) +
  geom_line() +
  theme_bw(12) +
  theme(axis.text.y = element_text(size=6)) +
  coord_flip() +
  ylab("Number of QTLs") +
  xlab("QTL dataset") +
  ggtitle("Number of QTLs at different thresholds")

pdf(paste0(output_root, ".qtl_numbers_at_different_thresholds.pdf"), width=12, height=10)
print(p)
dev.off()

p = ggplot(qtl_counts_flt, aes(x=fct_reorder(dataset, -gws_count), y=count, color=threshold, group=threshold)) +
  geom_line() +
  theme_bw(16) +
  theme(axis.text.x = element_blank()) +
  ylab("Number of QTLs") +
  xlab("QTL dataset") +
  ggtitle("Number of QTLs at different thresholds")

pdf(paste0(output_root, ".qtl_numbers_at_different_thresholds.nolabels.pdf"), width=12, height=7)
print(p)
dev.off()

qtl_counts_summary = qtl_counts %>%
  group_by(threshold) %>%
  summarise(total_count = sum(count),
            median_count = median(count))

print(qtl_counts_summary)

