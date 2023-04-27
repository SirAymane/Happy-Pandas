from pandas import DataFrame, read_csv
from pathlib import Path 
from matplotlib import pyplot 


def get_top_deaths_by_city(input_filename: str, catalogue: str, by_column: str) -> DataFrame:
    """ Creates a ranking based on the weekly deaths on a city.
    Firstly it reads the files, merges and querys the results sorting top 10.

    Args:
        input_file_name (str): Name of the deaths file inside the working directory defined.
        catalogue (str): Name of the catalogue file inside the working directory defined.
        by_column (str): Name of the column we would like to order by

    Returns:
        top_10_deaths -> DataFrame: Returns a sorted result based on the query on the merge of the files read.
    """

    # Getting path
    base_path: Path = Path(__file__).parent 
    deaths_path: Path = base_path/"clean_data"/input_filename
    nuts_catalogue_path: Path = base_path/"clean_data"/catalogue

    # Reading csv
    deaths: DataFrame = read_csv(deaths_path)
    nuts_catalogue: DataFrame = read_csv(nuts_catalogue_path)

    # Merge
    deaths_with_city: DataFrame = deaths.merge(nuts_catalogue[["nuts3_code", "nuts3_label"]],
                how= "left" ,left_on= "nuts", right_on= "nuts3_code")
    
    # Query
    deaths_2021: DataFrame = deaths_with_city.query("year == 2021")
    deaths_by_city  : DataFrame= deaths_2021.groupby([by_column]).agg({"deaths":"sum"}).reset_index()
    
    # Sort and return top 10.
    sorted_deaths : DataFrame = deaths_by_city.sort_values(by=["deaths"], ascending= False)
    top_10_deaths : DataFrame = sorted_deaths.head(10)
    return top_10_deaths



def show_ranking_deaths_by_city(input_file_name: str, catalogue: str, by_column: str) -> None:
    """ This function will display the ranking made with get_top_deaths, 
    That is to say a ranking of deaths by city.
    
    Since the function for the ranking is already defined earlier, we can call it.
    Once we read the ranking, we can display it with plot.bar taking by_column as X and Deaths on y.
    

    Args:
        input_file_name (str): Name of the deaths file inside the working directory defined.
        catalogue (str): Name of the catalogue file inside the working directory defined.
        by_column (str): Name of the column we would like to order by
    
    Returns:
        graph: Returns a visualization of the data, displayed in bar graph
    """
    # Reading ranking.
    top_10_deaths = get_top_deaths_by_city(input_file_name, catalogue, by_column)
    # Creating graph
    top_10_deaths.plot.bar(x = by_column, y = "deaths")
    # Displaying graph
    graph = pyplot.show()
    return graph



if __name__ == "__main__" :

    get_top_deaths_by_city('deaths_clean.csv', 'nuts3_clean.csv','nuts3_label')    
    show_ranking_deaths_by_city('deaths_clean.csv','nuts3_label')