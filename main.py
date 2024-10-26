import pandas as pd

df = pd.read_parquet("untracked-files/milestone01/metadata.parquet")
print(df.head())