import os
import sys
import requests
from multiprocessing import Pool
from datetime import datetime, timedelta
import re
import shutil

# Earthdata Login Credentials (hardcoded for demonstration; replace with your actual credentials)
EARTHDATA_USERNAME = "YOUR_CREDENTIAL_HERE"
EARTHDATA_PASSWORD = "YOUR_CREDENTIAL_HERE"

if len(sys.argv) != 4:
    print("Usage: python script.py <tilelist> <date_begin> <date_end>", file=sys.stderr)
    print("where <tilelist> is a text file of 5-character tile IDs", file=sys.stderr)
    print("<date_begin> and <date_end> are in the format 2021-12-31", file=sys.stderr)
    sys.exit(1)

tilelist = sys.argv[1]
datebeg = sys.argv[2]
dateend = sys.argv[3]

# A few customizable parameters
NP = 10  # Run this many download processes by default.

directory = f"data/originals_{datebeg}_{dateend}"
if os.path.exists(directory):
    shutil.rmtree(directory)  # Use shutil.rmtree() to remove the directory
    print(f"Removed directory: {directory}")



def download_file(url):
    filename = os.path.basename(url)
    
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url, auth=(EARTHDATA_USERNAME, EARTHDATA_PASSWORD), stream=True) as r:
        if r.status_code == 200:
            with open(os.path.join(directory, filename), 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("Downloaded File:", filename)
        else:
            print("Failed to download:", filename)

        
        
        
def process_tile(tile):
    for CLOUD in range(10, 100, 10):
        for spatial_cover in range(100, -1, -10):
            
            query = "https://cmr.earthdata.nasa.gov/search/granules.json"
            query += "?collection_concept_id=C2021957295-LPCLOUD&collection_concept_id=C2021957657-LPCLOUD"
            query += "&attribute[]=int,CLOUD_COVERAGE,," + str(CLOUD)
            query += "&attribute[]=string,MGRS_TILE_ID," + tile
            query += "&temporal=" + datebeg + "T00:00:00Z," + dateend + "T23:59:59Z"
            
            
            query += "&attribute[]=int,SPATIAL_COVERAGE," + str(spatial_cover) + ","
            files = []
            try:
                response = requests.get(query)
                # print(query)
                response.raise_for_status()
                metadata = response.json()
                for entry in metadata.get("feed", {}).get("entry", []):
                    links = entry.get("links", [])
                    for link in links:
                        # print(link)
                        href = link.get("href", "")
                        
                        start_pattern = re.compile(r'^https://data\.lpdaac\.earthdatacloud\.nasa\.gov')
                        end_pattern_b04 = re.compile(r'T\d{2}\w{3}\.\d{6,8}T\d+\.v\d+\.0\.B04\.tif$')
                        end_pattern_b05 = re.compile(r'T\d{2}\w{3}\.\d{6,8}T\d+\.v\d+\.0\.B05\.tif$')
        
                        if start_pattern.match(href) and (end_pattern_b04.search(href) or end_pattern_b05.search(href)):
                            files.append(href)
                if len(files)<2:
                    # print("Failed to retreive files for", tile)
                    continue
                
                files.sort(reverse = True)
                download_file(files[0])
                download_file(files[1])
                
                filename = os.path.basename(files[0])
                print(filename, "with spatial:", spatial_cover)
                
                return
                
            except Exception as e:
                print("Error processing tile:", tile)
                print(e)

# Process tiles in parallel
with Pool(NP) as pool:
    with open(tilelist, 'r') as f:
        tiles = f.read().splitlines()
        pool.map(process_tile, tiles)
