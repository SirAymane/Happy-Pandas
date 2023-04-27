from pandas import DataFrame, read_csv
from pathlib import Path 



def get_mortality_rate(deaths_filename: str, population_filename: str, catalogue_filename: str) -> None:
    """This functions reads the files, merge to unite them, filter with a query and calculate the values.

    Args:
        deaths_filename (str): The filename used to extract data of the deaths
        population_filename (str): The filename used to extract data of the population
        catalogue_filename (str): The filename used to extract data of the catalogue

    Return:
        File: the result is saved on the folder Results. If everything is correct, it will print a confirmation.
    """

    base_path: Path = Path(__file__).parent 

    # Saving the paths into variables
    deaths_path: Path = base_path/"clean_data"/deaths_filename
    population_path : Path = base_path/"clean_data"/population_filename
    catalogue_path : Path = base_path/"clean_data"/catalogue_filename

    # Reading csv files and saving them into a variable
    deaths: DataFrame = read_csv(deaths_path)
    population: DataFrame = read_csv(population_path)
    catalogue: DataFrame = read_csv(catalogue_path)


    # Here we're filtering to select the year 2021    
    deaths_2021: DataFrame = deaths.query("year == 2021")
    population_2021: DataFrame = population.query("year == 2021")


    # Groups by nuts column and then aggregates on deaths and population values
    deaths_by_city : DataFrame = deaths_2021.groupby(["nuts"]).agg({"deaths":"sum"}).reset_index()
    population_by_city : DataFrame = population_2021.groupby(["nuts"]).agg({"population":"sum"}).reset_index()



    # Merging previous df by same column, nuts
    deaths_population: DataFrame = deaths_by_city.merge(population_by_city, how= "left" , on= "nuts")
    deaths_population_with_regions: DataFrame = deaths_population.merge(catalogue[["nuts3_code", "nuts3_label"]], how= "left" , left_on= "nuts", right_on= "nuts3_code")

    # Calculating mortality_rate by 1000
    deaths_population_with_regions["mortality_rate"] = (deaths_population_with_regions["deaths"] / deaths_population_with_regions["population"]) * 1000
    mortality_rate_by_region : DataFrame = deaths_population_with_regions[["nuts3_label","mortality_rate"]]
    
    #Providing a confirmation
    print("# Exporting mortality_rate_by_region.csv ...")
    mortality_rate_by_region.to_csv(base_path/"results/mortality_rate_by_region.csv", index= False)
    print("# Exported succesfully.")



if __name__ == "__main__" :

    get_mortality_rate(input_file_name = 'deaths_clean.csv', selected_columns=['deaths', 'year', 'nuts'])









