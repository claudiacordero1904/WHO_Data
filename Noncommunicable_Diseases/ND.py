import requests
import pandas as pd



#Fetches all indicators from the GHO data
def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    #Fetch JSON data of all indicators available from GHO and convert it into a dataframe
    while url:
        print("Fetching indicators page", url)
        resp = requests.get(url)
        resp.raise_for_status
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@data.nextLink")

    ind_df = pd.DataFrame(all_rows)
    print("Total indicators found: ", len(ind_df))
    return ind_df


#Looks for noncommunicable disease specific indicators 
def find_ND_indicators(ind_df):
    search_words = ["overweight among children",
                    "overweight among adults",
                    "underweight among adults",
                    "thinness among children",
                    "obesity among children",
                    "tobacco use",
                    "Tobacco MPOWER",
                    "Diabetes",
                    "Physicial activity",
                    "Hypertension",
                    "Cholesterol",
                    "NCD Country Capacity",
                    "Cancer diagnosis and treatment",
                    "Cervical cancer screening",
                    "Palliative care",
                    "Vision and eyecare",
                    "Suicide rates",
                    "Cardiovascular diseases, cancer, diabetes",
                    "NCD"]

    pattern = "|".join(search_words)

    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique noncommunicable disease-specific indicators sorted by indicator codes
    ND_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n NCD Indicators found: ")
    print(ND_inds)

    return ND_inds

#Fetch data for noncommunicable disease indicators
def fetch_ND_data(ND_inds):
    ND_data = []

    #Fetches JSON data from each noncommunicable disease indicator using the indicator code
    for code in ND_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing noncommunicable disease data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        ND_data.append(df_i)

    #No data exists for this indicator code.
    if not ND_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    ND_df = pd.concat(ND_data, ignore_index=True)
    print(ND_df.head())
    return ND_df


def clean_and_reshape(ND_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]

    #Checks if any of the neccessary columns are missing in the ND dataframe
    missing = [c for c in needed_cols if c not in ND_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in ND_df ")

    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    ND_clean = (
        ND_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    ND_clean["YEAR"] = ND_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format noncommunicable disease data")
    print(ND_clean.head())

    #Transposes data to create a wide table 
    ND_wide = ND_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(ND_wide.head())

    return ND_clean, ND_wide


#Save data as csv files in both long and wide format data 
def save_outputs(ND_clean, ND_wide):
    ND_clean.to_csv("noncommunicable_disease_all_long.csv", index=False)
    ND_wide.to_csv("noncommunicable_disease_all_wide.csv")

    print("Saved noncommunicable_disease_all_long.csv and noncommunicable_disease_all_wide.csv")

if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    ND_inds = find_ND_indicators(ind_df)

    ND_df = fetch_ND_data(ND_inds)

    ND_long, ND_wide = clean_and_reshape(ND_df)

    save_outputs(ND_long, ND_wide)