import zipfile
import logging
import requests
import pandas as pd
from io import BytesIO
from src.reduce_mem import credentials


def get_frame() -> pd.DataFrame:
    req = requests.get(credentials['url'])
    logging.info(f"Download completed")
    zip_file = zipfile.ZipFile(BytesIO(req.content))
    df = pd.concat([pd.read_csv(zip_file.open(file_name.filename)) for file_name in zip_file.infolist()
                    if file_name.filename.endswith('.csv') and not file_name.filename.startswith('__MACOSX')], axis=0)
    df.reset_index(drop=True, inplace=True)
    logging.info(f"Formed DataFrame with shape: {df.shape}")
    df['date'] = pd.to_datetime(df['date'])
    return df
