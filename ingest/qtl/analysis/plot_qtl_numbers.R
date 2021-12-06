library(tidyverse)
theme_set(theme_bw())

args = commandArgs(trailingOnly=TRUE)
qvals_file = args[1]
output_root = args[2]

df = readr::read_csv(qvals_file)

df = df %>%
  mutate(dataset = paste(study_id, bio_feature, sep='-')) %>%
  mutate(type = if_else(grepl("sQTL", dataset), "sQTL", "eQTL"))

# Focus on eQTLs at first
sqtls = df %>% filter(type == "sQTL")
eqtls = df %>% filter(type == "eQTL")

# genes_tested = eqtls %>%
#   group_by(dataset) %>%
#   summarise(num_genes_tested = n())

# Get QTL counts at different thresholds
df_list = list()
thresholds = data.frame(name = c("p < 1e-4", "p < 1e-5", "p < 1e-6", "p < 5e-8"),
                        threshold = c(1e-4, 1e-5, 1e-6, 5e-8))
for (i in 1:nrow(thresholds)) {
  qtl_count = df  %>%
    group_by(dataset) %>%
    summarise(count = sum(min_pval < thresholds$threshold[i]),
              type = first(type))
  qtl_count$name = thresholds$name[i]
  df_list = c(list(qtl_count), df_list)
}

qtl_count_fdr5 = df  %>%
  group_by(dataset) %>%
  summarise(count = sum(qval < 0.05),
            type = first(type))
qtl_count_fdr5$name = "FDR 5%"

qtl_count_pbonf5 = df  %>%
  group_by(dataset) %>%
  summarise(count = sum(p_bonf < 0.05),
            type = first(type))
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

###########################################################################
# Plot eQTL counts
eqtl_counts_flt = qtl_counts_flt %>%
  filter(type == "eQTL")

p = ggplot(eqtl_counts_flt, aes(x=fct_reorder(dataset, gws_count), y=count, color=threshold, group=threshold)) +
  geom_line() +
  theme_bw(12) +
  theme(axis.text.y = element_text(size=6)) +
  coord_flip() +
  ylab("Number of eQTLs") +
  xlab("eQTL dataset") +
  ggtitle("Number of eQTLs at different thresholds")

pdf(paste0(output_root, ".eqtl_numbers_at_different_thresholds.pdf"), width=12, height=10)
print(p)
dev.off()

p = ggplot(eqtl_counts_flt, aes(x=fct_reorder(dataset, -gws_count), y=count, color=threshold, group=threshold)) +
  geom_line() +
  theme_bw(16) +
  theme(axis.text.x = element_blank()) +
  ylab("Number of eQTLs") +
  xlab("eQTL dataset") +
  ggtitle("Number of eQTLs at different thresholds")

pdf(paste0(output_root, ".eqtl_numbers_at_different_thresholds.nolabels.pdf"), width=12, height=7)
print(p)
dev.off()

qtl_counts_summary = qtl_counts %>%
  group_by(threshold) %>%
  summarise(total_count = sum(count),
            median_count = median(count))

print(qtl_counts_summary)


###########################################################################
# Plot spliceQTL counts
sqtl_counts_flt = qtl_counts_flt %>%
  filter(type == "sQTL")

p = ggplot(sqtl_counts_flt, aes(x=fct_reorder(dataset, gws_count), y=count, color=threshold, group=threshold)) +
  geom_line() +
  theme_bw(12) +
  theme(axis.text.y = element_text(size=6)) +
  coord_flip() +
  ylab("Number of sQTLs") +
  xlab("sQTL dataset") +
  ggtitle("Number of sQTLs at different thresholds")

pdf(paste0(output_root, ".sqtl_numbers_at_different_thresholds.pdf"), width=12, height=10)
print(p)
dev.off()

p = ggplot(sqtl_counts_flt, aes(x=fct_reorder(dataset, -gws_count), y=count, color=threshold, group=threshold)) +
  geom_line() +
  theme_bw(16) +
  theme(axis.text.x = element_blank()) +
  ylab("Number of sQTLs") +
  xlab("sQTL dataset") +
  ggtitle("Number of sQTLs at different thresholds")

pdf(paste0(output_root, ".sqtl_numbers_at_different_thresholds.nolabels.pdf"), width=12, height=7)
print(p)
dev.off()


###########################################################################
# sQTL vs eQTL counts in GTEx

qtl_counts_comp = qtl_counts_flt %>%
  filter(grepl("GTE", dataset)) %>%
  mutate(dataset = gsub("GTEx-eQTL-", "", dataset),
         dataset = gsub("GTEx-sQTL-", "", dataset)) %>%
  mutate(dataset = toupper(dataset))

p = ggplot(qtl_counts_comp %>% filter(threshold ==  "FDR 5%"),
           aes(x=fct_reorder(dataset, gws_count), y=count, color=type, group=type)) +
  geom_line() +
  theme_bw(16) +
  theme(axis.text.x = element_blank()) +
  ylab("Number of QTLs") +
  xlab("QTL dataset") +
  ggtitle("Number of QTLs at different thresholds")

pdf(paste0(output_root, ".gtex_eqtl_vs_sqtl_numbers_fdr_5_pct.nolabels.pdf"), width=10, height=7)
print(p)
dev.off()

# Print the number of QTLs at each threshold for GTEx eQTLs and sQTLs
qtl_counts_comp %>%
  group_by(type, threshold) %>%
  summarise(total_count = sum(count)) %>%
  pivot_wider(names_from = "type", values_from = "total_count")

qtl_counts_comp %>%
  group_by(type, threshold) %>%
  summarise(median_count = median(count)) %>%
  pivot_wider(names_from = "type", values_from = "median_count")
