import sys
import os
import pandas as pd
import numpy as np

# Function to get the size of an object
def get_size(obj):
    """Return the size of an object in bytes."""
    try:
        if isinstance(obj, pd.DataFrame):
            return obj.memory_usage(deep=True).sum()
        elif isinstance(obj, pd.Series):
            return obj.memory_usage(deep=True).sum()
        elif isinstance(obj, np.ndarray):
            return obj.nbytes
        else:
            return sys.getsizeof(obj)
    except Exception as e:
        print(f"Could not get size of object {obj}: {e}")
        return 0

# Create a list to store object names and sizes
object_info = []

# Get the current local namespace
current_objects = {name: obj for name, obj in globals().items() if not name.startswith('__') and not callable(obj)}

# Iterate through the objects in the current namespace
for name, obj in current_objects.items():
    obj_size = get_size(obj)
    
    object_info.append((name, type(obj).__name__, obj_size))
    
    # Print the object name and size in gigabytes
    print(f"Object: {name}, Type: {type(obj).__name__}, Size: {obj_size / (1024 ** 3):.4f} GB")

# Sort objects by size in descending order
object_info_sorted = sorted(object_info, key=lambda x: x[2], reverse=True)

# Create a DataFrame from the object info for easy viewing
object_info_df = pd.DataFrame(object_info_sorted, columns=['Object Name', 'Object Type', 'Size (bytes)'])

# Optionally, save the object information to a CSV file for detailed inspection
object_info_df.to_csv(root + 'Results/iota_summary_stats/object_memory_usage.csv', index=False)
