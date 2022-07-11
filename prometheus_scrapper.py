import os
import re
import argparse
from tqdm import tqdm
from prometheus_api_client.utils import parse_datetime, parse_timedelta
from prometheus_api_client import PrometheusConnect, MetricSnapshotDataFrame, MetricRangeDataFrame, PrometheusApiClientException


parser = argparse.ArgumentParser(description='Process to set parameters.')
parser.add_argument('--url', type=str, default='http://127.0.0.1:9090')
parser.add_argument('--start_time', type=str,default="3d")
parser.add_argument('--end_time', type=str,default='now')
parser.add_argument('--chunk_size', type=str,default='1d')
parser.add_argument('--storage_path', type=str,default='./prometheus_csv_non_preprocessing')
args = parser.parse_args()

def main(url:str, start_time:str, end_time:str, chunk_size:str, storage_path:str):
    prom = PrometheusConnect(url = url,disable_ssl=True)
    start_time = parse_datetime(start_time)
    end_time = parse_datetime(end_time)
    chunk_size = parse_timedelta(end_time, chunk_size)
    storage_path = storage_path

    if not os.path.isdir(storage_path):
        os.mkdir(storage_path)
    f =  open(storage_path +"/error_variable.txt",'w')

    # import avaliable metric's name
    variable_name = prom.all_metrics()

    # call data from prometheus
    for name in tqdm(variable_name,uint=" variable", desc=" scrpaing"):
        try:
            metric_data = prom.get_metric_range_data(
                metric_name=name,
                start_time=start_time,
                end_time=end_time,
                chunk_size=chunk_size)
            metric_df = MetricSnapshotDataFrame(metric_data)
            var_name = re.sub('[-=+,#/\?:^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·]', '_',name)
            metric_df = metric_df.rename(columns={'value':var_name})
            metric_df.to_csv(storage_path+"/"+var_name+".csv",header=True,index=False)

        except PrometheusApiClientException:
            print(name+" has too many samples so that occur PrometheusApiClientException. Check the "+storage_path+"/error_variable.txt.")
            f.write(name)
            pass
    f.close()
    
if __name__ == "__main__":
    url = args.url
    start_time = args.start_time
    end_time = args.end_time
    chunk_size = args.chunk_size
    storage_path = args.storage_path
    main(url,start_time,end_time,chunk_size,storage_path)