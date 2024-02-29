import logging
import pandas as pd
from functools import partial
from concurrent import futures
from typing import Iterator, List, Tuple


def calculate_chunk_size(count, chunk_size) -> Tuple:
    if chunk_size <= 0:
        logging.error("Chunk size cannot be zero or negative")
        raise ValueError("Chunk size cannot be zero or negative")
    ch_num = count//chunk_size
    if ch_num == 1:
        logging.error(f"Chunk {chunk_size} is too big for frame of size {count}")
        raise Exception(f"Chunk {chunk_size} is too big for frame of size {count}")
    remainder = count % chunk_size
    try:
        add_num = remainder//ch_num
        main_chunk_size = chunk_size+add_num
        second_remainder = remainder % ch_num
        last_chunk_size = chunk_size+add_num+second_remainder
        return main_chunk_size, last_chunk_size
    except ZeroDivisionError:
        logging.error(f"Chunk {chunk_size} is bigger than frame size {count}")
        raise Exception(f"Chunk {chunk_size} is bigger than frame size {count}")


def calculate_ranges(size, last_size, ind_max) -> List:
    ranges = [{*range(i, i+last_size)} if i+last_size == ind_max else {*range(i, i+size)}
              for i in range(0, ind_max-size, size)]
    return ranges


def select_indexes(inds, df, index_col=None) -> pd.DataFrame:
    if index_col:
        return df.loc[df[index_col].isin(inds), ~df.columns.isin([index_col])]
    return df.loc[df.index.isin(inds)]


def divide_into_chunks(frame, count_vals, chunk_size, index_col=None, max_workers=5) -> Iterator:
    size, last_size = calculate_chunk_size(count_vals, chunk_size)
    ranges = calculate_ranges(size, last_size, count_vals)
    args = partial(select_indexes, df=frame, index_col=index_col)
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        chunks = executor.map(args, ranges)
    return chunks


def chunker(frame, chunk_size, series_name=None, max_workers=5) -> Iterator:
    if not isinstance(frame, pd.DataFrame) or not frame.shape[0]:
        logging.error(f"Chunker can only be used on a pandas dataframe with data")
        raise Exception(f"Chunker can only be used on a pandas dataframe with data")
    if series_name:
        frame['rank'] = frame[series_name].rank(method='min')
        enum = dict([*enumerate(frame['rank'].unique())])
        keys = dict((v, k) for k, v in enum.items())
        frame['rank'] = frame['rank'].apply(lambda x: keys[x])
        del keys, enum
        count_vals = frame['rank'].max()+1
        chunks = divide_into_chunks(frame, count_vals, chunk_size, index_col='rank', max_workers=max_workers)
        frame.drop('rank', axis=True, inplace=True)
        return chunks
    count_vals = frame.shape[0]
    chunks = divide_into_chunks(frame, count_vals, chunk_size, max_workers=max_workers)
    return chunks
