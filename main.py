#!/usr/bin/env python3
import logging
import argparse
import src.reduce_mem as rm
import src.chunk_funcs as cf
from src.get_example_data import get_frame

creds = rm.credentials

parser = argparse.ArgumentParser(description='Chunker parameters')
parser.add_argument('--chunk_size', type=int, default=creds['chunk_size'], help='Desire chunk size')
parser.add_argument('--series_name', type=str, default=creds['series_name'], help='Name of series to split by')
parser.add_argument('--max_workers', type=int, default=creds['max_parallel_workers'], help='Number of threads')
args = parser.parse_args()


def run_chunker():
    try:
        df = get_frame()
        rm.reduce_memory_usage(df)
        chunks = cf.chunker(frame=df, chunk_size=args.chunk_size, series_name=args.series_name,
                            max_workers=args.max_workers)
        unpacked = [*chunks]
        logging.info(f"Divided dataframe into {len(unpacked)} chunks")
        return unpacked
    except MemoryError:
        logging.error("Memory error")
        raise Exception("Memory error")


if __name__ == '__main__':
    unpacked_chunks = run_chunker()
