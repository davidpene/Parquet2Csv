import pandas as pd
import sys
import os

folderPath = sys.argv[1]
folderName = folderPath.split("/")[-1]

outputDir = sys.argv[2]

# Create a folder in a specific directory.
os.makedirs(outputDir, exist_ok=True)

print(folderPath)

# Get a list of all files in the folder.
files = enumerate(os.listdir(folderPath))

# Iterate over the list of files and log the name of each file to the file.
for index, file in files:
    if(file.endswith('.parquet')):
        # Read the Parquet file using the fastparquet engine
        df = pd.read_parquet(f"{folderPath}/{file}", engine='fastparquet')

        # Write the DataFrame to a CSV file
        df.to_csv(f"{outputDir}/{index}_{folderName}.csv", index=False)
