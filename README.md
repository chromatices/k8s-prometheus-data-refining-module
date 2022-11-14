# k8s-prometheus-data-scraper    
- prometheus raw data scraper on k8s env    

This repository is a module for prometheus data scraping and preprocessing in k8s environments; the configuration consists of three files, each of which is summarized as follows:

**prometheus_scrapper.py**: Gets Prometheus metric data. Enter the Prometheus URL (ex: http://127.0.0.1:9090) in service, enter the starting point, end point, and collection interval of the collection period to import all the metric data provided by Prometheus.

**prometheus_preprocessing.py**: Collecting service metrics at the same time zone. Due to the nature of k8s, there are cases where the timestamp of metric data generated at the same time during the gathering process occurs in duplicate. Provides a collection of data that occurs within the same time; it is based on statistical techniques of sum, mean, and median.

**pod_separate.py**: Separate the data collected by service; proceeds with converting from metric-centered data to service-centered data.

------------
### Requirements
```
tqdm == 4.64.0
numpy == 1.21.5
pandas == 1.4.2
prometheus-api-client == 0.5.0
```

-----------------

### How to use
```
python3 prometheus_scrapper.py --url http://127.0.0.1:9090 --start_time 10d --end_time now --chunk_size [1s, 1t, 1h, 1d, 1m, 1y] --storage path [save dir]

python3 prometheus_preprocessing.py --storage_path [save_dir] --chunk_size [1s, 1t, 1h, 1d, 1m, 1y] --method [sum,mean,median]

python3 pod_separate.py --filename [prometheus_preprocessing.py 's output] --target_dir [prometheus_preprocessing.py 's output dir] --save_dir [final output dir]
```
------------------
- This work was supported by Institute for Information & communications Technology Promotion(IITP) grant funded by the Korea government(MSIT) (No.2021-0-00281, Development of highly integrated operator resource deployment optimization technology to maximize performance efficiency of high-load complex machine learning workloads in a hybrid cloud environment)

<!-- >>>>>>> aa9fb28b66f0adeadda5fcc24ea04af177947340 -->