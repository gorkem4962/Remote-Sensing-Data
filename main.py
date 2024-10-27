import pandas as pd
import re
from datetime import datetime

def get_season_from_patch_id(patch_id):
    # regular expression to search a 8 digit value
    match = re.search(r'\d{8}', patch_id)
    if match:
        date_str = match.group(0)
        date = datetime.strptime(date_str, "%Y%m%d")
        # Determine the season based on the month
        if date.month in [3, 4, 5]:
            return 'spring'
        elif date.month in [6, 7, 8]:
            return 'summer'
        elif date.month in [9, 10, 11]:
            return 'fall'
        else:
            return 'winter'
    return None


# calculating the maximum number of labels of one patch 
# and average number of labels for one patch 

def calculate_labels(labels):
    max_labels = max(len(patch) for patch in labels)

    total_labels = sum(len(patch) for patch in labels)
    average_labels = total_labels / len(labels)
    
    return max_labels, round(average_labels,2)






df = pd.read_parquet("untracked-files/milestone01/metadata.parquet")
# pd.set_option('display.max_colwidth', None)
# print(df.head())
df['season'] = df['patch_id'].apply(get_season_from_patch_id)

# Count the number of image patches per season
season_counts = df['season'].value_counts()

# Print the results in the required format
print(f"spring: {season_counts.get('spring', 0)} samples")
print(f"summer: {season_counts.get('summer', 0)} samples")
print(f"fall: {season_counts.get('fall', 0)} samples")
print(f"winter: {season_counts.get('winter', 0)} samples")

max_labels, average_labels = calculate_labels(df['labels'].tolist())

print("average-num-labels: AVG rounded to two decimals (%.2f)" % average_labels)