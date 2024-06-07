from multiprocessing import Pool
from tqdm import tqdm
import logging
import numpy as np
import pandas as pd
import requests
import os


def processFulltextDocuments(paired_path, port):
    for file_path, save_path in tqdm(paired_path, total=len(paired_path), desc=f'Processing files on port {port}'):
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

def assign_tasks(file_dir, save_dir, ports=[8086, 8186, 8286, 8386]):
    file_paths = [os.path.join(file_dir, filename) for filename in os.listdir(file_dir) if filename.endswith('.pdf')]
    save_paths = [os.path.join(save_dir, filename) for filename in os.listdir(file_dir) if filename.endswith('.pdf')]
    paired_path = list(zip(file_paths, save_paths))
    assigned_paired_path = np.array_split(paired_path, len(ports))
    pool = Pool(processes=len(ports))
    for i, port in enumerate(ports):
        pool.apply_async(processFulltextDocuments, args=(assigned_paired_path[i], port))
    logging.info('All tasks finished.')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    file_dir = '/public/home/bdpstu/xintianle/textbooks/libgen/test-pdfs'
    save_dir = '/public/home/bdpstu/xintianle/textbooks/libgen/grobid-output'
    os.makedirs(save_dir, exist_ok=True)
    assign_tasks(file_dir, save_dir, process_func=processFulltextDocuments)



