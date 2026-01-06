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


#Looks for World Health Statistic specific indicators 
def find_WHS_indicators(ind_df):
    search_words = ["Diarrhoea",
                    "Water-, santitation- and hygiene",
                    "Impoverishing health spending",
                    "Households impoverished",
                    "Households with out-of-pocket payments",
                    "Catastrophic health spending",
                    "Households pushed below or further below a relative poverty line",
                    "NCD country capacity",
                    "General availability of",
                    "Availability of cardiovascular risk stratifcation",
                    "Cancer diagnosis and treatment services",
                    "Physical activity",
                    "Hypertension",
                    "Overweight among children",
                    "Obesity among adults",
                    "Underweight among adults",
                    "Overweight among adults",
                    "Cholesterol",
                    "Paliative care",
                    "Beverage tax",
                    "Thinness among children",
                    "Diabetes",
                    "NCD deaths",
                    "Suicide rates",
                    "Dying between the exact ages",
                    "Vision and eyecare",
                    "Nursing personnel",
                    "Midwifery personnel",
                    "Medical doctors",
                    "Medical and Pathology Laboratory",
                    "Pharmacists",
                    "Pharmaceutical Technicians",
                    "Physiotherapists",
                    "Physiotherapy",
                    "Community health workers",
                    "Environmental and Occupational Health",
                    "Dentists",
                    "Dental Assistants and Therapists",
                    "Dental Prothetic Technicians",
                    "Traditional and complementary medicine",
                    "HIV",
                    "Mental Health",
                    "Health expenditure",
                    "Out-of-pocket expenditure",
                    "Child deaths",
                    "Child mortality",
                    "Adolescent mortality rate",
                    "Life expectancy",
                    "WASH",
                    "Yellow Fever",
                    "Measles",
                    "Rubella",
                    "Tetanus",
                    "Mumps",
                    "Diphtheria",
                    "Poliomyelitis",
                    "Rotavirus vaccines",
                    "immunization coverage",
                    "Vaccination cards",
                    "Japanese encephalitis",
                    "Pertussis",
                    "Congenital rubella syndrome",
                    "Stunting",
                    "Wasting",
                    "Overweight prevalence",
                    "Maternal mortality ratio",
                    "Births attended by skilled health personnel"]

    pattern = "|".join(search_words)

    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique World Health Statistic-specific indicators sorted by indicator codes
    WHS_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n World Health Statistics Indicators found: ")
    print(WHS_inds)

    return WHS_inds

#Fetch data for World Health Statistics indicators
def fetch_WHS_data(WHS_inds):
    WHS_data = []

    #Fetches JSON data from each World Health Statistics indicator using the indicator code
    for code in WHS_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing World Health Statistics data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        WHS_data.append(df_i)

    #No data exists for this indicator code.
    if not WHS_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    WHS_df = pd.concat(WHS_data, ignore_index=True)
    print(WHS_df.head())
    return WHS_df


def clean_and_reshape(WHS_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in WHS_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in WHS_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    WHS_clean = (
        WHS_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    WHS_clean["YEAR"] = WHS_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format World Health Statistics data")
    print(WHS_clean.head())

    #Transposes data to create a wide table 
    WHS_wide = WHS_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(WHS_wide.head())

    return WHS_clean, WHS_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(WHS_clean, WHS_wide):
    WHS_clean.to_csv("WHS_all_long.csv", index=False)
    WHS_wide.to_csv("WHS_all_wide.csv")

    print("Saved WHS_all_long.csv and WHS_all_wide.csv")

if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    WHS_inds = find_WHS_indicators(ind_df)

    WHS_df = fetch_WHS_data(WHS_inds)

    WHS_long, WHS_wide = clean_and_reshape(WHS_df)     

    save_outputs(WHS_long, WHS_wide)