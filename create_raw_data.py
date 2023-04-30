# Libraries
from pandas import DataFrame, read_csv, read_excel, Index
from pathlib import Path

#raw_data folder
def prepare_raw_data_folder() -> None:
    """Creates the eaw_data folder
    """
    
    current_path = Path(__file__).parent
    raw_data_path = current_path/"raw_data"
    
    try:
        raw_data_path.mkdir()
        print("raw_data folder has been created")
    except:
        print("Folder already exists")
 
# def remove_totals(data: DataFrame, col_name: str) -> DataFrame:
    
#     subsetted_data: DataFrame = data[~data[col_name].str.contains("TOTAL")]
#     subsetted_data: DataFrame = data[~data[col_name].str.contains("T,")]
    
#     return subsetted_data

#Fetch data functions
def get_weekly_death_dataset(url: str = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/demo_r_mweek3.tsv.gz") -> DataFrame:
    """The function fetches the zipped data from eurostat and returns a pandas dataframe

    Args:
        url (str, optional): URL to the main repository. Defaults to "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/demo_r_mweek3.tsv.gz".

    Returns:
        DataFrame: the original data parsed to dataframe
    """
    data = read_csv(url,
                    sep="\t",
                    compression="gzip",
                    encoding="utf8",
                    index_col=None)
    
    # For our mental health: we trim all the column names
    data = data.rename(columns=lambda x: x.strip())
    
    return data

def get_population_dataset(url: str = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/demo_r_pjangrp3.tsv.gz") -> DataFrame:
    """Fetch population dataset from Eurostat

    Args:
        url (str): _description_. Defaults to "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/demo_r_pjangrp3.tsv.gz".

    Returns:
        data (DataFrame): the fetched data in DataFrame format
    """
    data = read_csv(url,
                    sep="\t",
                    compression="gzip",
                    encoding="utf8",
                    index_col=None)
    
    # For our mental health: we trim all the column names
    data = data.rename(columns=lambda x: x.strip())
    
    return data

def get_nuts3_catalogue(url: str = "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2021.xlsx") -> DataFrame:
    """Fetch population data from Eurostat

    Args:
        url (_type_, optional): NUTS-3 Catalogue. Defaults to "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2021.xlsx".

    Returns:
        data (DataFrame): the fetched NUTS-3 Catalogue from Eurostat in DataFrame format
    """

    data = read_excel(url, sheet_name="NUTS & SR 2021", index_col=None)
    
    # For our mental health: we trim all the column names
    data = data.rename(columns=lambda x: x.strip())
    
    return data

def get_matching_columns(source_columns: list[str], model_columns: list[str]) -> list[str]:
    """Match the year-level columns between two DataFrames

    Args:
        source_columns (list[str]): columns from the source dataset to match
        model_columns (list[str]): matching columns

    Returns:
        matching columns list[str]: the columns to filter both DataFrames
    """
    
    matching_columns: list[str] = []
        
    for col in source_columns:
        year_name = col[0:4]
        
        if year_name in model_columns:
            matching_columns.append(col)

    return matching_columns

def subset_data_by_year(data: DataFrame, years: list[int]) -> DataFrame:
    """Filter the dataframe with a  temporal range

    Args:
        data (DataFrame): DataFrame to filter
        years (list[int]): target years

    Returns:
        subsetted data (DataFrame): DataFrame already filtered
    """
    
    data_col: list[str] = list(data.columns)
    
    target_col: list[str] = [data_col[0]]
    
    years_str = [str(x) for x in years]
    
    for col in data_col:
        if col in years_str:
            target_col.append(col)
            
    subsetted_data = data[target_col]
            
    return subsetted_data

def match_death_to_population(deaths: DataFrame, population: DataFrame) -> DataFrame:
    """Match death DataFrame columns with Population DataSet columns

    Args:
        deaths (DataFrame): Deaths dataset (year-week level)
        population (DataFrame): Population dataset (year level)

    Returns:
        subsetted_death_data (DataFrame): deaths DataSet with the matching range of population dataset
    """
    
    deaths_col_keys:        list[str] = list(deaths.columns)
    population_col_keys:    list[str] = list(population.columns)
    
    target_columns: list[str] = [deaths_col_keys[0]]
    target_columns.extend(get_matching_columns(deaths_col_keys, population_col_keys))
    
    subsetted_death_data = deaths[target_columns]
    
    return subsetted_death_data


def fill_raw_data_folder(target_folder: str="raw_data") -> None:
    """Creates and fill the raw_data folder with the primitive datasets from Eurostats

    Args:
        target_folder (str, optional): target directory. Defaults to "raw_data".
    Returns:
        None
    """
    
    print("# Preparing raw_data folder")
    prepare_raw_data_folder()
    
    target_folder = Path(__file__).parent/target_folder
    
    print("# Fetching deaths dataset...")
    deaths_data = get_weekly_death_dataset()
    print("# Deaths dataset fetched")
    
    print("# Fetching population dataset...")
    population_data = get_population_dataset()
    print("# Population dataset fetched")
    
    print("# Subsetting population data...")
    population_data_subset = subset_data_by_year(data=population_data, years=range(2020,2022)  )
    print("# Population data subsetted")
    
    print("# Equalize deaths data to population data year range")
    deaths_data_subset = match_death_to_population(deaths_data, population_data_subset)
    
    print("# Fetching nuts3 catalogue...")
    nuts3_catalogue = get_nuts3_catalogue()
    print("# NUTS3 catalogue fetched")
    
    deaths_data_subset.to_csv(target_folder/"deaths_data.csv", index=False, compression="gzip")
    print("# Deaths dataset exported!")
    
    population_data_subset.to_csv(target_folder/"population_data.csv", index=False, compression="gzip")
    print("# Population dataset exported")
    
    nuts3_catalogue.to_csv(target_folder/"nuts3_catalogue.csv", index=False, compression="gzip")
    print("# NUTS3 catalogue exported")


if __name__ == "__main__":
    
    fill_raw_data_folder()
