import pandas as pd
import sys
import os

folderPath = sys.argv[1]
outputDir = sys.argv[2]
columnName = sys.argv[3]
searchTerm = sys.argv[4]

folderName = folderPath.split("/")[-1]

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

        # Search for the value in the DataFrame
        search_result = df[df[columnName] == searchTerm]

        if (search_result[columnName].any()):
            # Convert the Series to a CSV file
            search_result.to_csv(f"{outputDir}/{index}_{folderName}.csv", index=False, header=True)
