from multiprocessing import Pool
from tqdm import tqdm
import logging
import numpy as np
import pandas as pd
import requests
import os


def processFulltextDocuments(paired_path, port):
    for file_path, save_path in paired_path:
        try:
            url = f'http://127.0.0.1:{port}/api/processFulltextDocument'
            with open(file_path, 'rb') as f:
                files = {'input': f}
                header = {'Accept': 'application/xml'}
                res = requests.post(url=url, files=files, headers=header, timeout=600)
            assert res.status_code == 200
            with open(save_path, 'wb') as f:
                f.write(res.content)
        except Exception as e:
            logging.error(f"Error processing {file_path} on port {port}: {e}")

def assign_tasks(file_dir, save_dir, ports=[8086, 8186, 8286, 8386]):
    file_paths = list(set([os.path.join(file_dir, filename) for filename in os.listdir(file_dir) if filename.endswith('.pdf')]))
    paired_path = [(file_path, os.path.join(save_dir, os.path.basename(file_path).replace('.pdf', '.xml'))) for file_path in file_paths]
    assigned_paired_path = np.array_split(paired_path, len(ports))
    logging.info(f'Assigning {len(paired_path)} tasks to {len(assigned_paired_path)} ports.')
    pool = Pool(processes=len(ports))
    results = []
    for i, port in enumerate(ports):
        results.append(pool.apply_async(processFulltextDocuments, args=(assigned_paired_path[i], port)))
    
    for res in tqdm(results):
        res.get()
    
    pool.close()
    pool.join()
    logging.info('All tasks finished.')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    file_dir = '/public/home/bdpstu/xintianle/textbooks/libgen/test-pdfs'
    save_dir = '/public/home/bdpstu/xintianle/textbooks/libgen/grobid-output'
    assign_tasks(file_dir, save_dir)


