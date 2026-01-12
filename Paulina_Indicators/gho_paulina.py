import requests
import pandas as pd
from pathlib import Path


def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    while url:
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@odata.nextLink") or js.get("@data.nextLink")

    ind_df = pd.DataFrame(all_rows)
    return ind_df


def find_all_topic_indicators(ind_df):
    search_words = [
        "Hepatitis",
        "HBV",
        "HCV",
        "HBsAg"
    ]

    pattern = "|".join(search_words)
    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    topic_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    return topic_inds


def fetch_topic_data(topic_inds):
    topic_data = []

    for code in topic_inds["IndicatorCode"]:
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        resp.raise_for_status()
        rows = resp.json().get("value", [])

        if not rows:
            continue

        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        topic_data.append(df_i)

    if not topic_data:
        raise SystemExit("No data found")

    topic_df = pd.concat(topic_data, ignore_index=True)
    return topic_df


def clean_and_reshape(topic_df):
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    missing = [c for c in needed_cols if c not in topic_df.columns]

    if missing:
        if missing == ["NumericValue"]:
            topic_df = topic_df.copy()
            topic_df["NumericValue"] = pd.NA
        else:
            raise ValueError(f"Missing columns: {missing}")

    topic_long = (
        topic_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
    )

    topic_long["YEAR"] = topic_long["YEAR"].astype(int)

    topic_wide = topic_long.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values="NumericValue"
    )

    return topic_long, topic_wide


def save_outputs(topic_long, topic_wide):
    outdir = Path(__file__).resolve().parent / "Hepatitis" / "output"
    outdir.mkdir(parents=True, exist_ok=True)

    topic_long.to_csv(outdir / "hepatitis_all_long.csv", index=False)
    topic_wide.to_csv(outdir / "hepatitis_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()
    topic_inds = find_all_topic_indicators(ind_df)
    topic_df = fetch_topic_data(topic_inds)
    topic_long, topic_wide = clean_and_reshape(topic_df)
    save_outputs(topic_long, topic_wide)
