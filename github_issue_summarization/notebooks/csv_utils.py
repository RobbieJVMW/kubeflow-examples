import logging
import pandas as pd


def sample_csv_file(data_file, sample_size, chunksize=100000):
    """Read CSV in chunks to avoid high memory usage."""
    df = pd.DataFrame([])
    # XXX: chunksize is practically raws
    logging.info("Sampling CSV file %s", data_file)
    total_rows = 0
    for chunk in pd.read_csv(data_file, chunksize=chunksize, low_memory=True):
        rows = len(chunk)
        total_rows += rows
        # FIXME: Should we somehow change the chunk sample size based
        # on the requested sample size?
        logging.debug("Processing %s rows (total %s)...", rows, total_rows)
        print('.', end='', flush=True)
        sample = chunk.sample(n=sample_size)
        df = df.append(sample)
        del chunk

    logging.info("Successfully sampled CSV file of total rows %s.", total_rows)
    return df.sample(n=sample_size)
