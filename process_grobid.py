from multiprocessing import Pool
from tqdm import tqdm
import logging
import numpy as np
import pandas as pd
import requests
import os


def processFulltextDocument(paired_path, port):
    file_path, save_path = paired_path
    url = f'http://127.0.0.1:{port}/api/processFulltextDocument'
    header = {'Accept': 'application/xml'}
    try:
        with open(file_path, 'rb') as f:
            files = {'input': f}
            res = requests.post(url=url, files=files, headers=header, timeout=600)
        res.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(res.content)
    except Exception as e:
        logging.error(f"Error processing {file_path} on port {port}: {e}")

def assign_tasks(file_dir, save_dir, ports=[8086, 8186, 8286, 8386]):
    # ports: grobid服务端口
    # file_dir: 待处理pdf文件目录
    # save_dir: 处理结果保存目录
    file_paths = [os.path.join(file_dir, filename) for filename in os.listdir(file_dir) if filename.endswith('.pdf') and not os.path.exists(os.path.join(save_dir, filename.replace('.pdf', '.xml')))]
    paired_paths = [(file_path, os.path.join(save_dir, os.path.basename(file_path).replace('.pdf', '.xml'))) for file_path in file_paths]
    logging.info(f'Assigning {len(paired_paths)} tasks to {len(ports)} ports.')
    
    pool = Pool(processes=len(ports))
    pbar = tqdm(total=len(paired_paths), desc="Processing tasks")
    update = lambda *args: pbar.update()
    for i, paired_path in enumerate(paired_paths):
        port = ports[i % len(ports)]
        result = pool.apply_async(processFulltextDocument, args=(paired_path, port), callback=update)
    
    pool.close()
    pool.join()
    logging.info('All tasks finished.')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    file_dir = '/public/home/bdpstu/username/pdfdir'
    save_dir = '/public/home/bdpstu/username/outputdir'
    assign_tasks(file_dir, save_dir)


