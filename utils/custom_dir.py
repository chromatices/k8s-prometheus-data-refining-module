import os

def custom_dir(storage_path):
    if not os.path.isdir(storage_path):
        os.mkdir(storage_path)