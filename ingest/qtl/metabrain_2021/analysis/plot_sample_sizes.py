import matplotlib.pyplot as plt
import pandas as pd

plt.hist([2.1, 3.1, 2.3, 6.4, 5.5, 7.2, 7.1, 8.7], 5)
plt.savefig('testplot.png')

chr22_sample = pd.read_csv('../raw/2020-05-26-Cortex-EUR-22-biogenformat.head50k.tsv', sep='\t')
#print(chr22_sample)

def sum_dataset_sample_sizes(sample_sizes_str):
    sample_sizes_list = sample_sizes_str.split(';')
    sizes_no_dash = list(filter(lambda s: s != '-', sample_sizes_list))
    sizes_ints = list(map(int, sizes_no_dash))
    return(sum(sizes_ints))


chr22_sample["total_sample_size"] = chr22_sample["DatasetsNrSamples"].apply(sum_dataset_sample_sizes)

plt.hist(chr22_sample["total_sample_size"], bins = 20)
plt.savefig('snp_sample_size_hist.Cortex-EUR-22.png')
plt.clf()

plt.hist(chr22_sample["total_sample_size"], density=True, histtype='step', cumulative=True)
plt.hist(chr22_sample["total_sample_size"], density=True, histtype='step', cumulative=-1)
plt.savefig('snp_sample_size_hist.Cortex-EUR-22.cdf.png')

#print(type(chr22_sample["total_sample_size"].value_counts()))
value_counts = pd.DataFrame(chr22_sample["total_sample_size"].value_counts())
value_counts.sort_index(ascending=False, inplace=True)
value_counts["sample size"] = value_counts.index
value_counts.rename(columns={'total_sample_size': 'SNP count'}, inplace=True)
value_counts.to_csv("sample_size_counts.tsv", sep='\t')
