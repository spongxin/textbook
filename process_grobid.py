from multiprocessing import Pool
from tqdm import tqdm
import logging
import numpy as np
import pandas as pd
import requests
import os


def processHeaderDocument(file_path, save_path, port):
    try:
        url = f'http://127.0.0.1:{port}/api/processHeaderDocument'
        files = {'input': open(file_path, 'rb')}
        header = {'Accept': 'application/xml'}
        res = requests.post(url=url, files=files, headers=header, timeout=600)
        assert res.status_code == 200
        with open(save_path, 'wb') as f:
            f.write(res.content)
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")

def processFulltextDocument(file_path, save_path, port):
    try:
        url = f'http://127.0.0.1:{port}/api/processFulltextDocument'
        files = {'input': open(file_path, 'rb')}
        header = {'Accept': 'application/xml'}
        res = requests.post(url=url, files=files, headers=header, timeout=600)
        assert res.status_code == 200
        with open(save_path, 'wb') as f:
            f.write(res.content)
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")

def assign_tasks(file_dir, save_dir, ports=[8086, 8186, 8286, 8386], process_func=processHeaderDocument):
    file_paths = [os.path.join(file_dir, filename) for filename in os.listdir(file_dir) if filename.endswith('.pdf')]

    with Pool(processes=len(ports)) as pool:
        for i, file_path in tqdm(enumerate(file_paths), total=len(file_paths), desc='Assigning tasks'):
            port = ports[i % len(ports)]
            pool.starmap(process_func, (file_path, os.path.join(save_dir, os.path.basename(file_path).replace('.pdf', '.xml')), port))
    logging.info('All tasks finished.')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    file_dir = '/public/home/bdpstu/xintianle/textbooks/libgen/test-pdfs'
    save_dir = '/public/home/bdpstu/xintianle/textbooks/libgen/grobid-output'
    os.makedirs(save_dir, exist_ok=True)
    assign_tasks(file_dir, save_dir, process_func=processFulltextDocument)


