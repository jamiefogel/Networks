# Note this is incomplete and half-assed code



# Determine the max value count for y-axis normalization
max_y = max(df_filtered['occ3'].value_counts().max(), df_filtered['iota'].value_counts().max(), df_filtered['mmc'].value_counts().max())

# Create the subplots
fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10, 15))

# Plotting histograms for each column with uniform y-axis
df_filtered['occ3'].value_counts().sort_index().plot(kind='bar', ax=axes[0], color='blue', title='Histogram of occ3', rot=90)
df_filtered['iota'].value_counts().sort_index().plot(kind='bar', ax=axes[1], color='green', title='Histogram of iota', rot=90)
df_filtered['mmc'].value_counts().sort_index().plot(kind='bar', ax=axes[2], color='red', title='Histogram of mmc', rot=90)

# Standardize the y-axis and potentially x-axis
axes[0].set_ylim(0, max_y)
axes[1].set_ylim(0, max_y)
axes[2].set_ylim(0, max_y)

# Setting labels
axes[0].set_ylabel('Counts')
axes[1].set_ylabel('Counts')
axes[2].set_ylabel('Counts')
axes[2].set_xlabel('Categories')

# Layout and show plot
plt.tight_layout()
plt.show()



df_filtered['occ3'].value_counts().head(20)
df_filtered['occ4'].value_counts().head(20)
df_filtered['iota'].value_counts().head(20)
df_filtered['mmc'].value_counts().head(20)

df_filtered['occ3'].value_counts().describe()
df_filtered['occ4'].value_counts().describe()
df_filtered['iota'].value_counts().describe()
df_filtered['mmc'].value_counts().describe()

sns.kdeplot(df_filtered['iota'].value_counts(), bw_adjust=0.5, label='Coef iota', color='blue', fill=True)
sns.kdeplot(df_filtered['mmc'].value_counts(), bw_adjust=0.5, label='Coef mmc', color='green', fill=True)
sns.kdeplot(df_filtered['occ3'].value_counts(), bw_adjust=0.5, label='Coef occ3', color='orange', fill=True)
plt.ylabel('Density')
plt.legend()
plt.show()