# Countries Data ETL Pipeline

A Python-based ETL pipeline that pulls live data for 250+ countries 
from the REST Countries API, cleans and structures it, and stores 
it in CSV, SQLite, and Parquet formats — ready for analysis.

---

## What This Project Does

Working with real-world API data is messy. Country records have 
nested fields, missing capitals, multiple currencies, and lists of 
languages packed into a single response. This pipeline handles all 
of that — extracting only what matters, cleaning it up, and loading 
it into structured formats.

---

## Technologies Used

- **Python 3.14**
- **Pandas** — data transformation and cleaning
- **SQLite** — structured local database storage
- **Parquet (PyArrow)** — compressed big data format
- **Requests** — REST API calls
- **REST Countries API** — free API with data for 250+ countries

---

## Project Structure

Countries_ETL/
│
├── etl_pipeline.py          
├── countries_data.csv       
├── countries_data.db        
├── countries_data.parquet   
└── README.md                


## How It Works
REST Countries API
│
│  Returns 250+ country records in JSON
▼
Extract Layer
│
│  Single API call fetches all countries
│  Status code validated before processing
▼
Transform Layer
│
│  Flattens nested JSON fields
│  Extracts first capital from list
│  Joins multiple languages into single string
│  Picks primary currency from nested object
│  Handles missing values with safe defaults
│  Validates population and area (no negatives)
▼
Load Layer
│
├── countries_data.csv
├── countries_data.db
└── countries_data.parquet

---

## Sample Output

| country | capital | region | population | area_km2 | currency_name | languages |
|---------|---------|--------|------------|----------|---------------|-----------|
| Ivory Coast | Yamoussoukro | Africa | 31719275 | 322463 | West African CFA franc | French |
| Italy | Rome | Europe | 60367477 | 301336 | Euro | Italian |
| Kyrgyzstan | Bishkek | Asia | 6524195 | 199951 | Kyrgyzstani som | Kyrgyz, Russian |
| Fiji | Suva | Oceania | 896444 | 18272 | Fijian dollar | English, Fijian, Hindi |
