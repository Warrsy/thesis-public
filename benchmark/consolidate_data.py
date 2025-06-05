import pandas as pd
import glob

all_files_recursive = glob.glob("results-from-exp/**/*_results.csv", recursive=True)

dataFrames = []

for filename in all_files_recursive:
    dataFrame = pd.read_csv(filename)
    
    parts = filename.split("_")
    model = parts[1]
    percentage = parts[2]
    
    dataFrame["model"] = model
    dataFrame["percentage"] = percentage
    
    dataFrame["elapsed_per_million_rows"] = (dataFrame["elapsed_millis"] / dataFrame["total_rows_processed"]) * 1e6
    dataFrame["peak_memory_per_million_rows"] = (dataFrame["peak_memory_mb"] / dataFrame["total_rows_processed"]) * 1e6
    
    if "error" not in dataFrame.columns:
        dataFrames.append(dataFrame)
    
combined_data = pd.concat(dataFrames, ignore_index=True)

combined_data.to_csv("combined_results_2.csv", index=False)