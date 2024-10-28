import pandas as pd
import re
from datetime import datetime
import dask.bag as db
from pathlib import Path
import rasterio
import matplotlib.pyplot as plt


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

def checking_correctness():

    return 0





df = pd.read_parquet("untracked-files/milestone01/metadata.parquet")
df['season'] = df['patch_id'].apply(get_season_from_patch_id)

# Count the number of image patches per season
season_counts = df['season'].value_counts()

# Print the results in the required format
print(f"spring: {season_counts.get('spring', 0)} samples")
print(f"summer: {season_counts.get('summer', 0)} samples")
print(f"fall: {season_counts.get('fall', 0)} samples")
print(f"winter: {season_counts.get('winter', 0)} samples")

# Calculate label statistics
max_labels, average_labels = calculate_labels(df['labels'].tolist())
print("average-num-labels: %.2f" % average_labels)

# Set up base path and list directory contents with Dask

file_path = Path('untracked-files/milestone01/BigEarthNet-v2.0-S2-with-errors/S2A_MSIL2A_20170720T100031_N9999_R122_T34UDG/S2A_MSIL2A_20170720T100031_N9999_R122_T34UDG_61_54/S2A_MSIL2A_20170720T100031_N9999_R122_T34UDG_61_54_B01.tif')

# Open the TIFF file
with rasterio.open(file_path) as src:
    # Read the data for Band 8A
    band_data = src.read(1)  # The `1` indicates the first (and possibly only) band
    
    # Print metadata for insights into the data structure
    print("Metadata:", src.meta)
    
    # Display the data array shape
    print("Data shape:", band_data.shape)
    
    # Visualize the band data with matplotlib
    plt.figure(figsize=(10, 10))
    plt.imshow(band_data, cmap='gray')
    plt.colorbar()
    plt.title("Band 8A - S2A_MSIL2A")
    plt.show() 