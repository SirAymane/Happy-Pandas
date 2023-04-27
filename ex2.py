from pathlib import Path
from pandas import DataFrame, read_csv, melt, options
from numpy import NaN
options.mode.chained_assignment = None

def remove_nr_value(data: DataFrame, col_name: str) -> DataFrame:
    """Function to remove 'NR' string from a variable

    Args:
        data (DataFrame): input data
        col_name (str): column to remove NR string

    Returns:
        DataFrame: same dataframe without NR string
    """
    
    cleaned_data: DataFrame = data
    
    cleaned_data [col_name] = cleaned_data[col_name].str.replace("NR,", "")
    
    return cleaned_data

def explode_variable(data: DataFrame, column_to_explode: str, returned_col_names: list[str]):
    """Function to convert to 

    Args:
        data (DataFrame): _description_
        column_to_explode (str): _description_
        returned_col_names (list[str]): _description_

    Returns:
        _type_: _description_
    """
    
    cleaned_data: DataFrame = remove_nr_value(data, column_to_explode)
    
    exploded_data: DataFrame = cleaned_data
    exploded_data[returned_col_names] = exploded_data[column_to_explode].str.split(",", expand=True)
    exploded_data = exploded_data.drop([column_to_explode], axis=1)
    
    return exploded_data

def pivot_longer(data: DataFrame, id_cols: list[str], new_variable_name: str, new_value_name: str) -> DataFrame:
    """Melt the columns (years, years-week) to variables

    Args:
        data (DataFrame): input data
        id_cols (list[str]): columns to mantain
        new_variable_name (str): the new variable name that will hold the melted variables
        new_value_name (str): the new variable name that will hols metled values

    Returns:
        pivoted_data (DataFrame): the pivoted DataFrame
    """
    
    pivoted_data: DataFrame = melt(data, id_vars=id_cols, var_name=new_variable_name, value_name=new_value_name)
    
    return pivoted_data

def get_p_variable(data: DataFrame, col_with_p:str) -> DataFrame:
    """Converts p string in value column to a new boolean column and remove the string

    Args:
        data (DataFrame): input DataFrame
        col_with_p (str): target column to remove p value

    Returns:
        data_p (DataFrame): DataFrame with values without p
    """
    
    data_p: DataFrame = data 
    data_p["is_provisional"] = data_p[col_with_p].str.contains("p")
    data_p[col_with_p] = data_p[col_with_p].str.replace(" p", "")
    data_p[col_with_p] = data_p[col_with_p].str.replace(" ep", "")
    data_p[col_with_p] = data_p[col_with_p].str.replace(" e", "")
    
    return data_p

def fix_indicator(data: DataFrame, indicator_name: str) -> DataFrame:
    """Fix and parse the indicator column to numerical value

    Args:
        data (DataFrame): input DataFrame
        indicator_name (str): name of the indicator column in input DataFrame

    Returns:
        cleaned_data (DataFrame): cleaned DataFrame
    """
    
    cleaned_data: DataFrame = data
    cleaned_data.loc[cleaned_data[indicator_name].str.contains(":"), indicator_name] = NaN
    cleaned_data[indicator_name] = cleaned_data[indicator_name].astype('Int32')
    
    return cleaned_data

def filter_nuts3_level(data: DataFrame, nuts_col_name: str) -> DataFrame:
    """Filters and return the NUTS-3 catalogue in tidy format with the relational values

    Args:
        data (DataFrame): input DataFrame
        nuts_col_name (str): low-level column of NUTS-3

    Returns:
        filtered_data (DataFrame): the filtered DataFrame
    """
    
    filtered_data: DataFrame = data.query(f"{nuts_col_name}.str.len() == 5")
    
    return filtered_data

def expand_year_week(data: DataFrame, year_week_col: str) -> DataFrame:
    """Explodes year-week string variable to year and week column separatly

    Args:
        data (DataFrame): input DataFrame
        year_week_col (str): column to explode

    Returns:
        expanded_data (DataFrame): _description_
    """
    
    expanded_data: DataFrame = data
    
    expanded_data["year"] = expanded_data[year_week_col].str[0:4]
    expanded_data["week"] = expanded_data[year_week_col].str[5:]
    
    return expanded_data

def remove_totals(data: DataFrame) -> DataFrame:
    """Removes aggregated rows (Totals)

    Args:
        data (DataFrame): input DataFrame

    Returns:
        filtered_data (DataFrame): the filtered DataFrame
    """
    
    filtered_data: DataFrame = data
    filtered_data = filtered_data.query("sex != 'T'")
    filtered_data = filtered_data.query("age != 'TOTAL'")
    
    return filtered_data

def collapse_columns(data: DataFrame, columns_to_collapse: list[str], collapsed_col_name: str) -> DataFrame:
    """Collapse columns for tidy format

    Args:
        data (DataFrame): input DataFrame
        columns_to_collapse (list[str]): names of columns to collapse
        collapsed_col_name (str): new name of the collapsed column

    Returns:
        collapsed_data (DataFrame): the collapsed DataFrame
    """
    
    collapsed_data: DataFrame = data 
    
    collapsed_data[collapsed_col_name] = collapsed_data[columns_to_collapse].agg(lambda x: "".join(x.values), axis=1)
    
    return collapsed_data

def tidy_deaths_dataset(input_file_name: str, output_file_name: str) -> None:
    """Exports the tidy deaths DataFrame

    Args:
        input_file_name (str): input file name
        output_file_name (str): location to outpu the tidy data
    """
    
    print("-"*20)

    base_path: Path = Path(__file__).parent 
    file_path: Path = base_path/"raw_data"/input_file_name
    result_file_path: Path = base_path/"tidy_data"/output_file_name
    
    print(f"# Reading dataset: '{input_file_name}'...")
    data: DataFrame = read_csv(file_path, index_col=None, engine="pyarrow", compression="gzip")
    
    print("# Exploding variables...")
    exploded_data: DataFrame = explode_variable(data, "unit,sex,age,geo\\time", ["sex", "age", "nuts"])
    
    print("# Reshaping to long format...")
    long_format_data: DataFrame = pivot_longer(exploded_data, ["sex", "age", "nuts"], "year_week", "deaths")
    
    print("# Creting 'is_provisional' column (bool)...")
    p_data: DataFrame = get_p_variable(long_format_data, "deaths")
    
    print("# Fixing indicator column...")
    cleaned_data: DataFrame = fix_indicator(p_data, "deaths")
    
    print("# Filtering by NUTS3 level rows...")
    filtered_data: DataFrame = filter_nuts3_level(cleaned_data, "nuts")
    
    print("# Creating year and week variables...")
    expanded_data: DataFrame = expand_year_week(filtered_data, "year_week")
    
    print("# Removing totals...")
    tidy_data: DataFrame = remove_totals(expanded_data)
    
    print(f"# Exporting: {output_file_name}")
    tidy_data.to_csv(result_file_path, compression="gzip", index=False)
    print(f"# {output_file_name} exported!")
    
    print(f"# Columns of the dataset {output_file_name}")
    print(tidy_data.head())
    
    return

def tidy_population_dataset(input_file_name: str, output_file_name: str) -> None:
    """Exports the tidy population DataFrame

    Args:
        input_file_name (str): input file name
        output_file_name (str): location to outpu the tidy data
    """
    
    print("-"*20)

    base_path: Path = Path(__file__).parent 
    file_path: Path = base_path/"raw_data"/input_file_name
    result_file_path: Path = base_path/"tidy_data"/output_file_name
    
    print(f"# Reading dataset: '{input_file_name}'...")
    data: DataFrame = read_csv(file_path, index_col=None, engine="pyarrow", compression="gzip")
    
    print("# Exploding variables...")
    exploded_data: DataFrame = explode_variable(data, "sex,unit,age,geo\\time", ["sex", "age", "nuts"])
    
    print("# Reshaping to long format...")
    long_format_data: DataFrame = pivot_longer(exploded_data, ["sex", "age", "nuts"], "year", "population")
    
    print("# Creting 'is_provisional' column (bool)...")
    p_data: DataFrame = get_p_variable(long_format_data, "population")
    
    print("# Fixing indicator column...")
    cleaned_data: DataFrame = fix_indicator(p_data, "population")
    
    print("# Filtering by NUTS3 level rows...")
    filtered_data: DataFrame = filter_nuts3_level(cleaned_data, "nuts")
    
    print("# Removing totals...")
    tidy_data: DataFrame = remove_totals(filtered_data)
    
    print(f"# Exporting: {output_file_name}")
    tidy_data.to_csv(result_file_path, compression="gzip", index=False)
    print(f"# {output_file_name} exported!")
    
    print(f"# Columns of the dataset {output_file_name}")
    print(tidy_data.head())
    
    return
    
def tidy_nuts_catalogue(input_file_name: str, output_file_name: str) -> None:
    """Exports the tidy NUTS DataFrame

    Args:
        input_file_name (str): input file name
        output_file_name (str): location to outpu the tidy data
    """
    
    print("-"*20)
    
    base_path: Path = Path(__file__).parent 
    file_path: Path = base_path/"raw_data"/input_file_name
    result_file_path: Path = base_path/"tidy_data"/output_file_name
    
    print(f"# Reading dataset: '{input_file_name}'...")
    data: DataFrame = read_csv(file_path, index_col=None, engine="pyarrow", compression="gzip")
    
    print("# Collapsing NUTS levels")
    collapsed_data: DataFrame = collapse_columns(data, ["Country", "NUTS level 1", "NUTS level 2", "NUTS level 3"], "nuts_label")
    
    print("# Selecting columns...")
    modal_data: DataFrame = collapsed_data[["Code 2021", "nuts_label"]]
    
    print("# Subsetting NUTS-3 level...")
    nuts3_data: DataFrame = modal_data.query("`Code 2021`.str.len() == 5")
    
    print("# Renaming dataframe...")
    nuts3_data = nuts3_data.rename(columns={"Code 2021": "nuts3_code", "nuts_label": "nuts3_label"})
    
    print("# Upper aggregation codes...")
    print("## NUTS-2 code")
    nuts3_data["nuts2_code"] = nuts3_data["nuts3_code"].str.slice(0,4)
    print("## NUTS-1 code")
    nuts3_data["nuts1_code"] = nuts3_data["nuts3_code"].str.slice(0,3)
    print("## Country code")
    nuts3_data["country_code"] = nuts3_data["nuts3_code"].str.slice(0,2)
    
    print("# Adding NUTS-2 labels...")
    nuts2_label: DataFrame = modal_data.rename(columns={"Code 2021": "nuts2_code", "nuts_label": "nuts2_label"})
    nuts3_data = nuts3_data.merge(nuts2_label, how="left", left_on="nuts2_code", right_on="nuts2_code")
    
    print("# Adding NUTS-1 labels...")
    nuts1_label: DataFrame = modal_data.rename(columns={"Code 2021": "nuts1_code", "nuts_label": "nuts1_label"})
    nuts3_data = nuts3_data.merge(nuts1_label, how="left", left_on="nuts1_code", right_on="nuts1_code")
    
    print("# Adding Country labels...")
    country_label: DataFrame = modal_data.rename(columns={"Code 2021": "country_code", "nuts_label": "country_label"})
    nuts3_data = nuts3_data.merge(country_label, how="left", left_on="country_code", right_on="country_code")
    
    print("# Rearranging columns...")
    rearranged_data: DataFrame = nuts3_data[["nuts3_code", "nuts2_code", "nuts1_code", "country_code", "nuts3_label", "nuts2_label", "nuts1_label", "country_label"]]
    
    print(f"# Exporting {output_file_name}...")
    rearranged_data.to_csv(result_file_path, index=False)
    print(f"# {output_file_name} exprted!")
    
    print(f"# Columns of the dataset {output_file_name}")
    print(rearranged_data.head())
    
    return