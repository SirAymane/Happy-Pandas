from pandas import DataFrame, read_csv
from pathlib import Path 

def merge_dataframes(deaths_file: str, nuts_file: str): 
    """This function retrieves in a Dataframe the data from a csv file containing deaths numbers merged
    with the corresponding nuts region data existing in the nuts csv file,

    Args:
        deaths_file (str): The csv file containing the deaths related data
        nuts_file (str): The csv file containing the corresponding nuts region info
    """

    base_path: Path = Path(__file__).parent 
    # Get the csv files
    deaths_path: Path = base_path/"clean_data"/deaths_file
    nuts_path: Path = base_path/"clean_data"/nuts_file

    # Create dataframes from the csv files
    print("# Generating dataframes from files...")
    deaths: DataFrame = read_csv(deaths_path, index_col = None)
    nuts: DataFrame = read_csv(nuts_path, index_col = None)

    # Merge the dataframes as described and print it
    print("# Merging dataframes...")
    deaths_with_labels: DataFrame = deaths.merge(nuts, how="left", left_on=["nuts"], right_on=["nuts3_code"]) 
    print(deaths_with_labels)


if __name__ == "__main__" :

    merge_dataframes()

