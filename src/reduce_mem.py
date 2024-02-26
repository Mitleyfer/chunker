import yaml
import logging
import numpy as np
from functools import partial


with open('./config.yaml', 'r') as file:
    credentials = yaml.safe_load(file)


logging.basicConfig(
    filename=credentials['logs_path'],
    level=logging.INFO,
    format='%(asctime)s :: %(name)s - %(levelname)s - %(message)s')


def reduce_memory_column(col, num_rows, df) -> None:
    col_type = df[col].dtypes.name
    if col_type == 'object':
        cardinality = len(df[col].unique())
        if num_rows / cardinality > 150:
            df[col] = df[col].astype('category')
    elif col_type in {'int16', 'int32', 'int64', 'float16', 'float32', 'float64'}:
        c_min = (df[col].min(), 0)[int(df[col].isnull().values.all())]
        c_max = (df[col].max(), 0)[int(df[col].isnull().values.all())]
        if col_type[:3] == 'int':
            if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                df[col] = df[col].astype(np.int8)
            elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                df[col] = df[col].astype(np.int16)
            elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                df[col] = df[col].astype(np.int32)
        else:
            if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                df[col] = df[col].astype(np.float16)
            elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                df[col] = df[col].astype(np.float32)


def reduce_memory_usage(df) -> None:
    start_mem = df.memory_usage(deep=True).sum()/1024**2
    shape = df.shape
    args = partial(reduce_memory_column, num_rows=shape[0], df=df)
    _ = [*map(args, df.columns)]
    end_mem = df.memory_usage(deep=True).sum()/1024**2
    mem_reduction_pct = 100*(start_mem-end_mem)/start_mem
    logging.info(f'Mem. usage decreased from {start_mem:.2f} to {end_mem:.2f} Mb ({mem_reduction_pct:.1f}% reduction)')
