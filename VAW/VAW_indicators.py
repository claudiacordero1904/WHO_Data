import requests
import pandas as pd


#fetch all existing indicators in the WHO GHO data

def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    #Retrieves JSON data from the GHO indicators page 
    while url:
        print("Fetching Indicator page:", url)
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@odata.nextLink")
    
    #Converts the rows of indicator data into a pandas dataframe
    ind_df = pd.DataFrame(all_rows)
    print(ind_df)
    print("Total indicators: ", len(ind_df))
    return ind_df

#Filters through "Violence Against Women" indicators 
def find_all_VAW_indicators(ind_df):
    #Filters through indicator names to check for titles of indicators listed under the 
    # "Violence Against Women" category in the GHO
    #"Violence: intimate partner violence prevalence among ever partnered women in their lifetime (%)" is the only indicator with
    #valid numeric data so that is the only one we are able to use for the "Violence Against Women" category


    search_words = ["intimate partner violence", "non-partner sexual violence prevalence"]
    
    pattern = '|'.join(search_words)
    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)
   
    VAW_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index()
    )

    print("\n Violence Against Women indicators found: ")
    print(VAW_inds)
    print("Number of VAW indicators: ", len(VAW_inds))
    return VAW_inds

#Fetch data for each VAW indicator
def fetch_all_VAW_data(VAW_inds):
    VAW_data = []

    #Fetch indicator codes straight from the JSONified data
    for code in VAW_inds["IndicatorCode"]:
        print("\nFetching data for:", code)
        url= f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)

        #Status code of 200 means everything is working properly. We must bypass alternative circumstances.
        if resp.status_code != 200:
            continue

        js = resp.json()
        rows = js.get("value", [])
        if not rows:
            print("No data, skipping", code)
            continue

        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        VAW_data.append(df_i)

    if not VAW_data:
        raise SystemExit("No pollution data collected. Check API connection.")
    
    VAW_df = pd.concat(VAW_data, ignore_index=True)
    print("Shape :", VAW_df.shape)
    print(VAW_df)
    return VAW_df


def clean_and_reshape(VAW_df):
    #Include the necessary columns from the original indicator table and check for missing data
    needed_cols=["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    missing = [c for c in needed_cols if c not in VAW_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in VAW_df")
    
    #Create a new table with the cleaned data (removed columns with missing data and renamed variables)
    #Country data will not include ISO3 codes as only global regions are included in the API's data
    VAW_clean = (
        VAW_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
    )

    #Ensure the year is numeric
    VAW_clean["YEAR"] = VAW_clean["YEAR"].astype(int)
    print("\n Clean long-format Violence Against Women data ")
    
    #Print first 5 rows of the long format data 
    print(VAW_clean.head())

    #Create wide-format table by transposing the data
    VAW_wide = VAW_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values="NumericValue"
    )

    print(VAW_wide)
    return VAW_clean, VAW_wide


#save csv and excel files for both long and wide-format data

def save_outputs(VAM_clean, VAM_wide):
    VAM_clean.to_csv("VAM_all_long.csv", index=False)
    VAM_wide.to_csv("VAM_all_wide.csv")

    print("Created long data file: VAM_all_long.csv")
    print("Created wide data csv file: VAM_all_wide.csv")



if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    VAW_inds = find_all_VAW_indicators(ind_df)

    VAW_raw = fetch_all_VAW_data(VAW_inds)

    VAW_long, VAW_wide = clean_and_reshape(VAW_raw)

    save_outputs(VAW_long, VAW_wide)




    








    








