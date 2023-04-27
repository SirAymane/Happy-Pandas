from pandas import DataFrame, read_csv
from pathlib import Path


def show_selected_columns(input_file_name: str, selected_columns: list[str]) -> DataFrame:
    """This function will show only some of the columns of the df.

    Args:
        input_file_name (str): This will be the csv file location.
        selected_columns (list[str]): this corresponds to the columns we want to see.

    Returns:
        selected_deaths: The result will be the input_file_name file read, with the
                            columns we are interested in, showing the head of it.
    """

    base_path: Path = Path(__file__).parent 
    file_path: Path = base_path/"clean_data"/input_file_name
    # Reads CSV
    deaths: DataFrame = read_csv(file_path)
    selected_deaths: DataFrame = deaths[selected_columns]
    return selected_deaths.head(10)


def filter_rows(input_file_name: str) -> DataFrame:
    """This function takes an input file and returns the file with a filter.
    To make the filter we are using a mask which is applied on the df.
    The mask will replace the values to be only if the condition is true.
    In this case the filter is the age ranges 15-19 and 85-89

    Args:
        input_file_name (str): this will be the location of the file.

    Returns:
        DataFrame: the return will be the result of the input with a mask
                    showing the head only of the resulting df.
    """

    base_path: Path = Path(__file__).parent 
    file_path: Path = base_path/"clean_data"/input_file_name
    # Reads CSV
    deaths: DataFrame = read_csv(file_path)
    # Applying mask to our df.
    mask: DataFrame = (deaths['age'] == 'Y15-19') | (deaths['age'] == 'Y85-89')
    result: DataFrame = deaths.loc[mask]
    return result.head(10)



if __name__ == "__main__" :

    show_selected_columns(input_file_name = 'deaths_clean.csv', selected_columns=['deaths', 'year', 'nuts'])

    filter_rows(input_file_name= 'deaths_clean.csv')




