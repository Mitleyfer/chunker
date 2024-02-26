### Description

Chunker for pandas DataFrame. Divide data by specified serie name (row index if not specified) into
chunks of the desired size or bigger. Throws exceptions if chunk size is too big or bigger than size
of serie unique values.

Parameters of chunking can be specified either in command line or in **config.yaml**. If len of dataframe
is big and chunk size is very small specify bigger number of *max_parallel_workers* - it
is number of threads for chunker to process data concurrently.

Example dataset is provided from https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data.
The link to the smallest dataset (year 2013) is provided, to avoid memory allocation errors.

### Dependencies

- python == 3.11.5
- pyarrow == 11.0.0
- pandas == 2.0.3
- numpy == 1.24.3
- pytest == 7.4.0
- requests == 2.31.0
- pyyaml == 6.0