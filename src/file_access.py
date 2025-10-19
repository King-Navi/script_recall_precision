import pandas as pl 



def read(filepath) -> pl.DataFrame:
    df = pl.read_csv(filepath)
    return df

