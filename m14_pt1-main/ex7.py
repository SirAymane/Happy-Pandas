from pandas import DataFrame, cut, read_csv
from pathlib import Path 
from matplotlib import pyplot


def get_categories(deaths_file: str, ranges: list[float], categories: list[str]) -> DataFrame : 
    """This function applies categories to mortality rates depending on the ranges entered and shows them in the 'mortality_cat' column

    Args:
        deaths_file (str): The file containing the deaths related data
        ranges (list[float]): The ranges that should be applied to the data in the specified column
        categories (list[str]): The names of the categories to be assigned to each of the ranges created

    Returns:
        mortality_rate (DataFrame): the output dataframe with the previous actions applied 
    """

    base_path: Path = Path(__file__).parent 
    mortality_rate_path: Path = base_path/"results"/deaths_file 
    print("# Creating mortality_rate dataframe...")
    mortality_rate: DataFrame = read_csv(mortality_rate_path, index_col = None)
    print("# Creating column 'mortality_cat' with categories for mortality rate defined ranges...")
    mortality_rate["mortality_cat"] = cut(mortality_rate.mortality_rate, ranges, right = False, labels = categories) 
    return mortality_rate




def show_mortality_rates(filename: str, ranges: list[float], categories: list[str]) -> None:
    """This function shows a bars graph containing the amount of regions that fall in each mortality rate category.

    Args:
        filename (str): The file containing the categorized mortality rates
        ranges (list[float]): The ranges that should be applied to the data in the specified column
        categories (list[str]): The names of the categories to be assigned to each of the ranges created
    """
    print("# Generating graph...")
    mortality_cat: DataFrame = get_categories(filename, ranges, categories)
    categories_count: DataFrame = mortality_cat.groupby(["mortality_cat"]).size().reset_index(name = "Regions count")
    categories_count.plot.bar(x = "mortality_cat", y = "Regions count")

    pyplot.show()




if __name__ == "__main__" :

    show_mortality_rates("mortality_rate_by_region.csv", [0, 9, 12, 100], ["Below average", "Average", "Over average"])