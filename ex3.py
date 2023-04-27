from pathlib import Path 
from pandas import DataFrame, read_csv
import shutil

def remove_non_informative_rows(input_file_name: str, output_file_name: str, indicator_column: str) -> None:
    """Removes and filters the DataFrame from non informative rows. Exports clean DataFrame

    Args:
        input_file_name (str): input file name
        output_file_name (str): output location
        indicator_column (str): indicator column
    """
    
    print("-"*20)

    base_path: Path = Path(__file__).parent 
    file_path: Path = base_path/"tidy_data"/input_file_name
    result_file_path: Path = base_path/"clean_data"/output_file_name
    
    print(f"# Reading {input_file_name}...")
    data: DataFrame = read_csv(file_path, compression="gzip", index_col=None, engine="pyarrow")
    
    n_rows: int = len(data)
    print(f"# {input_file_name} initial count of rows: {n_rows}")
    
    print(f"# Removing NaN rows based on '{indicator_column}' variable...")
    non_nan_data: DataFrame = data.dropna(subset=[indicator_column])
    
    print(f"# Removing 0 value rows based on '{indicator_column}' variable...")
    non_0_data: DataFrame = non_nan_data[non_nan_data[indicator_column] > 0]
    
    n_final_rows: int = len(non_0_data)
    print(f"# {output_file_name} final number of rows: {n_final_rows}")
    
    print(f"# Exporting {output_file_name}...")
    non_0_data.to_csv(result_file_path, index=False)
    print(f"# {output_file_name} exported!")
    
    return

def copy_file(target_file: str, dest_file: str):
    """Cpy file from directory

    Args:
        target_file (str): target file name to copy
        dest_file (str): destination
    """
    
    print("-"*20)
    
    base_path: Path = Path(__file__).parent 
    file_to_copy_path: Path = base_path/target_file
    copied_file_path: Path = base_path/dest_file
    
    shutil.copyfile(file_to_copy_path, copied_file_path)
    print(f"# {target_file} has been copied to {copied_file_path}!")
    return
