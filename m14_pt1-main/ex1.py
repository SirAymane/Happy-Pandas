from pandas import DataFrame, read_csv
from pathlib import Path
import gzip

def get_row_count(file: str) -> int:
    """Count rows of a file in a stream way

    Args:
        file (str): input file name

    Returns:
        rows (int): count of rows 
    """
    
    base: Path = Path(__file__).parent 
    target_file: Path = base/"raw_data"/file
    
    rows: int = sum(1 for line in gzip.open(target_file)) -1
    
    return rows

def get_count_nas(file: str) -> int:
    """Count NaN ocurrences in file

    Args:
        file (str): input file name

    Returns:
        na_count int: NaN count
    """
    
    base: Path = Path(__file__).parent 
    target_file: Path = base/"raw_data"/file
    
    data: DataFrame = read_csv(target_file, compression="gzip")
    na_data: DataFrame = data[data.isna().any(axis=1)]
    
    na_count: int = len(na_data.index)
    
    return na_count


def print_top_rows(file: str) -> None:
    """Print top rows of DataFrame

    Args:
        file (str): file name
    
    Returns:
        None, just a head of a DataFrame
    """
    
    base: Path = Path(__file__).parent 
    target_file: Path = base/"raw_data"/file
    
    data: DataFrame = read_csv(target_file, compression="gzip")
    
    print(data.head())
