import pytest
import random
import string
import pandas as pd
import src.chunk_funcs as ch_funcs


dfs_test = pd.date_range("2023-01-01 00:00:00", "2023-01-01 00:01:00", freq="s")
df_test = pd.DataFrame({"dt": dfs_test.repeat(10), "val": ''})
df_test['val'] = df_test['val'].apply(lambda x: ''.join(random.choices(string.ascii_uppercase+string.digits,
                                                                       k=random.choice([*range(3, 12)]))))


def test_chunks_consistency():
    df_test_copy = df_test.copy()
    chunks = ch_funcs.chunker(df_test_copy, 10, series_name='dt', max_workers=5)
    unpacked = [*chunks]
    df_test_copy = pd.concat(unpacked)
    assert df_test_copy.equals(df_test)


def test_empty_dataframe():
    with pytest.raises(Exception, match="Chunker can only be used on a pandas dataframe with data"):
        df_empty = pd.DataFrame()
        chunks = ch_funcs.chunker(df_empty, 10, series_name='', max_workers=5)
        unpacked = [*chunks]
        return unpacked


def test_is_dataframe():
    with pytest.raises(Exception, match="Chunker can only be used on a pandas dataframe with data"):
        df_list = [1, 2, 3]
        chunks = ch_funcs.chunker(df_list, 10, series_name='', max_workers=5)
        unpacked = [*chunks]
        return unpacked


def test_chunk_size():
    with pytest.raises(Exception, match="Chunk 45 is too big for frame of size 61"):
        chunks = ch_funcs.chunker(df_test, 45, series_name='dt', max_workers=5)
        unpacked = [*chunks]
        return unpacked


def test_chunk_size_bigger_than_df():
    with pytest.raises(Exception, match="Chunk 200 is bigger than frame size 61"):
        chunks = ch_funcs.chunker(df_test, 200, series_name='dt', max_workers=5)
        unpacked = [*chunks]
        return unpacked


def test_chunk_size_less_or_equal_to_zero():
    with pytest.raises(ValueError, match="Chunk size cannot be zero or negative"):
        chunks = ch_funcs.chunker(df_test, -1, series_name='dt', max_workers=5)
        unpacked = [*chunks]
        return unpacked
