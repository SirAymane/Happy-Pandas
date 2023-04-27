from pandas import DataFrame, to_datetime, read_csv
from pathlib import Path 
from matplotlib import pyplot as plt


def get_deaths_by_week(deaths_file: str, catalogue_file: str, population_file: str) -> DataFrame: 
    """This function merges a deaths dataframe with its corresponding catalogue dataframe of regions as well as 
    its corresponding population dataframe into a new dataframe. 

    Then, the mortality rate is calculated and inserted in the 'mortality_rate' column. 

    Finally, the 'date' column data is formated as Date type so it can be used later as such. 

    Args:
        deaths_file (str): The file containing the deaths related data
        catalogue_file (str): The file containing the corresponding regions catalogue
        population_file (str): The file containing the corresponding population file

    Returns:
        deaths_population (DataFrame): The resulting dataframe with the mortality rate data, organized by country 
        and week of the year, correctly formatted.
    """

    base_path: Path = Path(__file__).parent 
    # Create deaths dataframe
    print("# Generating deaths dataframe from files...")
    deaths_path: Path = base_path/"clean_data"/deaths_file
    deaths: DataFrame = read_csv(deaths_path, index_col = None)
    deaths = deaths.query('week!=99') # We get rid of not dated deaths.

    # Create catalogue dataframe
    print("# Generating catalogue dataframe from files...")
    nuts_catalogue_path: Path = base_path/"clean_data"/catalogue_file
    nuts_catalogue: DataFrame = read_csv(nuts_catalogue_path, index_col = None)

    # Create population dataframe
    print("# Generating population dataframe from files...")
    population_path: Path = base_path/"clean_data"/population_file
    population: DataFrame = read_csv(population_path, index_col = None) 

    # Aggregate deaths by nuts country code and week of the year, then add the country labels from the catalogue
    print("# Merging dataframes...")
    deaths_by_year: DataFrame = deaths.groupby(["nuts","year_week","year"]).agg({"deaths":"sum"}).reset_index()
    deaths_by_year_with_country: DataFrame = deaths_by_year.merge(nuts_catalogue[["nuts3_code", "country_label"]], how= "left", left_on="nuts", right_on="nuts3_code") 

    # Aggregate deaths again in the new dataframe by week and year, but this time also by country label
    deaths_by_year_by_country: DataFrame = deaths_by_year_with_country.groupby(["country_label", "year_week", "year"]).agg({"deaths":"sum"}).reset_index()

    # Aggregate population by nuts country code and by year,
    population_by_year: DataFrame = population.groupby(["nuts","year"]).agg({"population":"sum"}).reset_index()

    # Add country label from catalogue
    population_with_country: DataFrame = population_by_year.merge(nuts_catalogue[["nuts3_code", "country_label"]], how= "left", left_on="nuts", right_on="nuts3_code") 

    # Aggregate population again in the new dataframe by year, but this time also by country label 
    population_by_country: DataFrame = population_with_country.groupby(["country_label", "year"]).agg({"population":"sum"}).reset_index() 

    # Create resulting dataframe by merging deaths by year and country dataframe with population by country and year dataframe
    deaths_population: DataFrame = deaths_by_year_by_country.merge(population_by_country, how="left", on=["country_label", "year"])

    # Calculate and insert mortality rate into the resulting dataframe
    print("# Calculating mortality rates...")
    deaths_population['mortality_rate'] = (deaths_population["deaths"] / deaths_population["population"]) * 1000 

    # Format the column with the weeks of the year to Date type to be used later and return the dataframe
    print("# Formatting dates...")
    deaths_population['date'] = to_datetime(deaths_population["year_week"] + "-1", format = "%GW%V-%u", errors = "ignore")
    return deaths_population


def show_mortality_rates_by_week(deaths_file: str, catalogue_file: str, population_file: str, countries_list: list) -> None:
    """ This function uses the data existing in the 'date' column from a dataframe to show a lines chart comparing the evolution 
    of the mortality rate among different countries defined in a list

    Args:
        deaths_file (str): The file containing the deaths related data
        catalogue_file (str): The file containing the corresponding regions catalogue
        population_file (str): The file containing the corresponding population file
        countries_list (list): The list of countries defined to be compared
    """

    # Get dataframe with the deaths by week data
    mortality_time_series: DataFrame = get_deaths_by_week(deaths_file, catalogue_file, population_file)
    # Search the defined countries to be compared in the dataframe and retrieve their data
    mortality_time_series = mortality_time_series.query("country_label==@countries_list")

    # Show the mortality rate evolution for the defined countries
    print("# Generating graph...")
    fig, ax = plt.subplots()
    for key, grp in mortality_time_series.groupby(['country_label']):
        ax = grp.plot(ax=ax, kind='line', x='date', y='mortality_rate', label=key)
    plt.show()


if __name__ == "__main__" :

    show_mortality_rates_by_week("deaths_clean.csv", "nuts3_clean.csv", "population_clean.csv, ['España', 'France', 'Portugal', 'Italia', 'Deutschland', 'Suomi/Finland']")


