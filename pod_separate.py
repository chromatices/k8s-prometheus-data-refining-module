import os
import argparse
import numpy as np
import pandas as pd
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Process to set parameters.')
parser.add_argument('--filename',type=str,default=None)
parser.add_argument('--target_dir',type=str,default=None)
parser.add_argument('--save_dir',type=str,default=None)
args = parser.parse_args()

def custom_dir(storage_path):
    if not os.path.isdir(storage_path):
        os.mkdir(storage_path)

def pod_separate(csv_file:str, dir:str, save_dir:str):
    custom_dir(save_dir)
    core_csv = pd.read_csv(dir+csv_file)
    pod_list = core_csv['pod'].unique()
    for pod in tqdm(pod_list, uint=" pod", desc=" integrating" ):
        tmp = core_csv[core_csv['pod']==pod]
        tmp = tmp.sort_values(by=['timestamp'],axis=0).reset_index(drop=True)
        tmp = tmp.drop(['pod'],axis=1)
        drop_list=[]
        for i in range(len(tmp.columns[1:])):
            if tmp.describe().iloc[-1,i]==np.nan:                
                drop_list.append(tmp.columns[i+1])
        drop_list = list(set(drop_list))
        tmp = tmp.drop(drop_list,axis=1)
        tmp = tmp.fillna(1e-9)
        tmp.to_csv(save_dir+pod+'.csv',index=False)

if __name__ == '__main__':
    file = args.filename
    dir = args.target_dir
    save_dir = args.save_dir
    pod_separate(file,dir,save_dir)