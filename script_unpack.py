# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 21:04:01 2020

@author: chest
"""

import os
import gzip
import pandas as pd
from tqdm import tqdm
import pickle 

## Change directory and get all sub-directories
os.chdir(r'C:\Users\chest\Desktop\Comfort Delgro')
subdirs = [x for x in os.walk(r'C:\Users\chest\Desktop\Comfort Delgro\2009')]

## Get all files in directory and load them into a list of dataframe
list_df = []
for dir in tqdm(subdirs):
    if len(dir[2]) > 0:
        path = dir[0]
        files = dir[2]
        for i in files:
            file_path = f"{path}\{i}"
            with gzip.open(file_path) as file:
                df = pd.read_csv(file, sep='\t')
                list_df.append(df)

## Merge all dataframe read
master_df = pd.concat(list_df)

## Check if dataframe has missing values
master_df.isna().sum()

## Notice that Size has missing values
## Entire group of product is missing

missing_df = master_df[master_df.Size.isna()]

## Save the DF file as pickle object
with open("master_df.pkl", "wb") as file:
    pickle.dump(master_df, file)