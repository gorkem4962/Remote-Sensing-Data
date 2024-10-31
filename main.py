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

def count_children(parent_directory):
    parent = Path(parent_directory)
    # Use list comprehension to get only directories
    child_directories = [p for p in parent.iterdir() if p.is_dir()]
    return len(child_directories)

def checkup_metadata(path_patch_id, meta_data, counter):
    path_patch_id = path_patch_id.split('/')[-1]
    for patch_id_parquet in meta_data['patch_id']:
        
        if patch_id_parquet == path_patch_id:
            return counter  # Return if a match is found
        
   
    return counter +1 # Ensure counter is returned even if no match is found



def calculate_num_of_errors(path,meta_parquet):
    num_wrongsize = 0
    num_nodata = 0
    num_npdataset = 0
    
    band_resolution = {
        "B02": "10m/px", "B03": "10m/px", "B04": "10m/px", "B08": "10m/px",
        "B05": "20m/px", "B06": "20m/px", "B07": "20m/px", "B8A": "20m/px",
        "B11": "20m/px", "B12": "20m/px", "B01": "60m/px", "B09": "60m/px"
    }
    
    pattern = r"B(?:\d{2}|8A)"
    
    for sub_path in path.iterdir():
        if not sub_path.is_dir():
            continue
        for sub_path_order in sub_path.iterdir():
            num_npdataset = checkup_metadata(str(sub_path_order),meta_parquet,num_npdataset)

            if not sub_path_order.is_dir():
                continue
            for sub_sub_path in sub_path_order.iterdir():
                match = re.findall(pattern, str(sub_sub_path))
                if match:
                    right_value = band_resolution.get(match[0])
                    with rasterio.open(str(sub_sub_path)) as dataset:
                        transform = dataset.transform
                        meters_per_pixel_x = round(transform.a)
                        meters_per_pixel_x_str = f"{meters_per_pixel_x}m/px"
                        
                        if meters_per_pixel_x_str != right_value:
                            num_wrongsize += 1

                        nodata_value = dataset.nodata
                        if nodata_value is not None:
                            nodata_mask = dataset.read_masks(1) == 0
                            if nodata_mask.any():
                                num_nodata += 1 
                else:
                    print("No matches found.")
                
                

    return num_wrongsize, num_nodata, num_npdataset




 
 




path = Path("untracked-files/milestone01/BigEarthNet-v2.0-S2-with-errors")
df = pd.read_parquet("untracked-files/milestone01/metadata.parquet")

df['season'] = df['patch_id'].apply(get_season_from_patch_id)

# Count the number of image patches per season
season_counts = df['season'].value_counts()
# Calculate label statistics
max_labels, average_labels = calculate_labels(df['labels'].tolist())



# Print the results in the required format
print(f"spring: {season_counts.get('spring', 0)} samples")
print(f"summer: {season_counts.get('summer', 0)} samples")
print(f"fall: {season_counts.get('fall', 0)} samples")
print(f"winter: {season_counts.get('winter', 0)} samples")


print("average-num-labels: %.2f" % average_labels)

num_wrong_size,num_no_data,num_np_dataset = calculate_num_of_errors(path,df)
print("wrong-size: " + str(num_wrong_size))

print("with-no-data: " + str(num_no_data)) 
print("not-part-of-dataset: " + str(num_np_dataset))



