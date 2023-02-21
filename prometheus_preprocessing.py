import argparse
import os

import pandas as pd
from nostril import generate_nonsense_detector
from tqdm import tqdm
from utils.custom_dir import custom_dir

parser = argparse.ArgumentParser(description='Process to set parameters.')
parser.add_argument('--storage_path', type=str,default='./AGC/')
parser.add_argument('--chunk_size', type=str, default='H', help="set chunk size fot rounding timestamp. 'D' means day, 'H' means hour, 'T' means minute 's' means seconds.")
parser.add_argument('--method', type=str,default='mean', help= "choose groupby method ex) mean, sum, median")
args = parser.parse_args()



def preprocessing_each_pod(dir:str, chunk_size:str,method:str):
    
    assert method in ['sum','mean','median'], "Choose groupby method on ['sum','mean',median']. If you wanna apply another function, edit the method preprocessing_each_pod."
    
    pod_dir = dir + 'each_pod_csv/'
    custom_dir(pod_dir)
    file_list = os.listdir(dir)        
    count=0
    
    # integration all metric csv files for preprocessing
    for file in tqdm(file_list, unit=" variable", desc=" preprocessing"):
        if file.split('.')[-1] == 'csv':
            if count==0:
                core_df = pd.read_csv(dir + file)
                if 'pod' in core_df.columns:
                    core_df = core_df.loc[:,['timestamp','pod',file.split('.')[0]]]
                    count+=1
                else:
                    pass
            else:
                add_df = pd.read_csv(dir + file)
                if add_df[file.split('.')[0].split('Prometheus_metric_')[-1]].sum() != 0.0:
                    if 'pod' in add_df.columns:
                        add_df = add_df.loc[:,['timestamp','pod',file.split('.')[0]]]
                        core_df = pd.concat([core_df,add_df],axis=0)

    #timestamp to yy-mm-dd format
    core_df['timestamp']=pd.to_datetime(core_df['timestamp'],unit='s') 
    # round time by set unit
    core_df['timestamp'] = core_df['timestamp'].apply(lambda x: x.round(freq=chunk_size)) 
    # replace na value from pod 
    core_df['pod'] = core_df['pod'].fillna(0) 
    # remove metric that has no pod name
    core_df = core_df[core_df['pod']!=0] 
    
    # remove hash that in pod name 
    core_df['label'] = core_df['pod'].apply(lambda x: 1 if x.split('-')[-1].isalpha() == False else 0)
    num_df = core_df[core_df['label']==1]
    alpha_df = core_df[core_df['label']==0]

    nonsense_detector = generate_nonsense_detector(min_length=4, min_score=4.2)
    num_df['pod'] = num_df.apply(lambda x: x['pod'].replace('-' + x['pod'].split('-')[-1],''), axis=1)        
    alpha_df['pod'] = alpha_df.apply(lambda x: x['pod'].replace('-' + x['pod'].split('-')[-1],'') if nonsense_detector(x['pod'].split('-')[-1]) else x['pod'], axis=1)    
    
    core_df = pd.concat([num_df,alpha_df],axis=0)

    # integration value at same time
    core_df_groupby = core_df.groupby(['timestamp','pod'])
    if method == 'sum':
        core_df_split = core_df_groupby[core_df.columns[1:]].sum().reset_index(drop=False)    
    elif method == 'mean':
        core_df_split = core_df_groupby[core_df.columns[1:]].mean().reset_index(drop=False)
    elif method == 'median':
        core_df_split = core_df_groupby[core_df.columns[1:]].median().reset_index(drop=False)
    
    
    # drop column that has only null value.
    na_list=core_df_split.isnull().sum()[core_df_split.isnull().sum()>int(len(core_df_split)//2)]
    core_df_split= core_df_split.drop(list(na_list.index),axis=1)
    core_df_split.to_csv(pod_dir+"pod_total_"+ chunk_size +'_'+method+".csv",index=False)



if __name__ == "__main__":
    dir = args.storage_path
    chunk = args.chunk_size
    method = args.method
    preprocessing_each_pod(dir,chunk,method)