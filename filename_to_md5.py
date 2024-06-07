from tqdm import tqdm
import pandas as pd
import hashlib
import os


def get_md5(file_path):
    with open(file_path, 'rb') as f:
        md5 = hashlib.md5(f.read()).hexdigest()
    return md5

def get_filename_md5_table(file_dir):
    filename_md5_table = {}
    file_paths = [os.path.join(file_dir, filename) for filename in os.listdir(file_dir)]
    for file_path in tqdm(file_paths):
        md5 = get_md5(file_path).upper()
        filename_md5_table[md5] = os.path.basename(file_path)
    return pd.DataFrame(filename_md5_table.items(), columns=['md5', 'filename'])

if __name__ == '__main__':
    file_dir = '/public/home/bdpstu/xintianle/textbooks/libgen/test-pdfs'
    filename_md5_table_path = '/public/home/bdpstu/xintianle/textbooks/libgen/test_pdfs_md5_table.csv'
    filename_md5_table = get_filename_md5_table(file_dir)
    filename_md5_table.to_csv('filename_md5_table.csv', index=False)