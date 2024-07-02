from multiprocessing import Pool
from tqdm import tqdm
import requests
import argparse
import logging
import time
import os


def processFulltextDocument(paired_path: tuple[str, str], port: int, **kwargs):
    file_path, save_path = paired_path
    url = kwargs.get('url', 'http://127.0.0.1')
    api = f'{url}:{port}/api/processFulltextDocument'
    header = {'Accept': 'application/xml'}
    try:
        with open(file_path, 'rb') as f:
            file = {'input': f}
            res = requests.post(url=api, files=file, headers=header, timeout=kwargs.get('timeout', 600))
        res.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(res.content)
    except Exception as e:
        logging.error(f"Error processing {file_path} on port {port}: {e}")

def assign_tasks(file_dir: str, save_dir: str, ports, **kwargs):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    file_paths = [os.path.join(file_dir, filename) for filename in os.listdir(file_dir) if filename.endswith('.pdf')]
    paired_paths = [(file_path, os.path.join(save_dir, os.path.basename(file_path).replace('.pdf', '.xml'))) for file_path in file_paths]
    logging.info(f'Assigning {len(paired_paths)} tasks to ports: {ports}.')

    start = time.time()
    pool = Pool(processes=min(len(ports), len(paired_paths)))
    pbar = tqdm(total=len(paired_paths), desc="Processing tasks")
    update = lambda *args: pbar.update()
    for i, paired_path in enumerate(paired_paths):
        if not kwargs.get('force', False) and os.path.exists(paired_path[1]):
            logging.info(f"File {paired_path[1]} already exists, skipped.")
            update()
            continue
        port = ports[i % len(ports)]
        res = pool.apply_async(processFulltextDocument, args=(paired_path, port), callback=update)
    
    pool.close()
    pool.join()
    end = time.time()
    logging.info('All tasks finished in %.2f seconds.', end - start)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, help='path to the directory containing PDF files')
    parser.add_argument('--output', type=str, required=False, default='output', help='path to the directory where to put the results(optional)')
    parser.add_argument('--port', required=False, action='append', type=int, help='port number of the GROBID service(optional)')
    parser.add_argument('--force', action='store_true', help='force re-processing pdf input files when tei output files already exist')
    parser.add_argument('--verbose', action='store_true', help='print information about processed files in the console')
    args = parser.parse_args()
    assign_tasks(args.input, args.output, args.port)

