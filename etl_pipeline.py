import requests
import json
import pandas as pd
import sqlite3
import os

        #  Extract Part  #

def extract():
    print("Extracting data from API....")

    url = "https://restcountries.com/v3.1/all?fields=name,population,area,region,subregion,capital,languages,currencies,flags,borders"

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print(f"Successfully extracted {len(data)} countries ")
        return data
    else: 
        print("Error:", response.status_code)
        return None

        #  Transform Part  #

def transform(data):
    print("\nTransforming data")

    records = []

    for country in data:
        name = country.get("name", {}).get("common", "Unknown")

        capital = country.get("capital", ["No Capital"])
        capital = capital[0] if capital else "No Capital"

        region = country.get("region", "Unknown")
        subregion = country.get("subregion", "Unknown")

        population = country.get("population", 0)
        area = country.get("area", 0.0)

        currencies = country.get("currencies", {})
        if currencies:
            first_currency = list(currencies.keys())[0]
            currency_name = currencies[first_currency].get("name","Unknown")
            currency_symbol = currencies[first_currency].get("symbol", "Unknown")
        else :
            currency_name = "No Currency"
            currency_symbol = "No Currency"

        languages = country.get("languages", [])
        languages = ", ".join(languages.values()) if languages else "Unknown"

        borders = country.get("borders", [])
        borders = ", ".join(borders) if borders else "No Borders"

        flag_url = country.get("flags",{}).get("png", "No Flag")

        if population < 0:
            population = 0
        if area < 0:
            area = 0.0 

        records.append({
            "country": name,
            "capital": capital,
            "region": region,
            "subregion": subregion,
            "population": population,
            "area_km2": area,
            "currency_name": currency_name,
            "currency_symbol": currency_symbol,
            "languages": languages,
            "borders": borders,
            "flag_url": flag_url
        })

    df = pd.DataFrame(records)

    print(f"Transformed {len(df)} countries")
    print(f"Columns: {list(df.columns)}")
    return df


        #  Load Part  #

def load(df):
    print("\nLoading data..")

                #  CSV  #

    df.to_csv("countries_data.csv", index = False)
    print("Data saved to countries_data.csv")

            #  Load to SQLITE  #

    conn = sqlite3.connect("countries_data.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS countries(
            country TEXT,
            capital TEXT,
            region TEXT,
            subregion TEXT,
            population INTEGER,
            area_km2 REAL,
            currency_name TEXT,
            currency_symbol TEXT,
            languages TEXT,
            borders TEXT,
            flag_url TEXT
        )
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO countries VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,(
            row["country"],
            row["capital"],
            row["region"],
            row["subregion"],
            row["population"],
            row["area_km2"],
            row["currency_name"],
            row["currency_symbol"],
            row["languages"],
            row["borders"],
            row["flag_url"]
        )
    )

    conn.commit()
    conn.close()
    print("Data saved to SQLite Database")


            #  Load to Parquet  #

    df.to_parquet("countries_data.parquet", index = False)
    print("Data saved to Parquet")

            #  MAIN FUNCTION #

data = extract()

if data is None:
    print("No data returned from API!")
else:
    print("FIrst Country sample:", data[0])
    df = transform(data)
    load(df) 