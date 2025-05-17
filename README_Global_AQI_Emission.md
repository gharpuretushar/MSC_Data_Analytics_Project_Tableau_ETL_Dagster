# 🌍 Global Insights into Greenhouse Gases and Air Quality (A Per Capita Perspective)

A comprehensive data-driven project that explores the complex relationships between **Greenhouse Gas Emissions**, **Air Quality (AQI)**, and **Per Capita Income** across countries from **2000 to 2021**. This work integrates multiple datasets, builds ETL pipelines, and generates visual insights to aid policymakers, researchers, and environmental analysts in identifying pollution trends and health risks.

---

## 📌 Project Overview

This project addresses the key question:

> _Does higher per capita income necessarily correlate with worse air quality and higher emissions?_

We investigate this through regression analysis, pollutant-level breakdowns, and dashboard visualizations of global trends using emissions, AQI, and socio-economic indicators.

---

## 🛠️ Tech Stack

- **Languages**: Python (Pandas, Requests, SQLAlchemy)
- **ETL Tool**: Dagster
- **Databases**: PostgreSQL, MongoDB (Atlas)
- **Visualization**: Tableau
- **APIs**: KAPSARC (Greenhouse Emissions API)
- **Orchestration**: Dagster Pipelines

---

## 🗂️ Project Structure

| File/Folder | Description |
|-------------|-------------|
| `ETL.py` | Full DAG-based ETL pipeline using Dagster. Automates ingestion, transformation, and storage across MongoDB and PostgreSQL. |
| `AQI.ipynb` | Data analysis on global AQI trends and pollutants. |
| `Emission.ipynb` | Analysis of annual emissions dataset and insights. |
| `green_house.ipynb` | JSON-based greenhouse gas analysis and conversion for integration. |
| `RegressionTestForResearchQuestion.ipynb` | Regression models evaluating AQI vs Per Capita Emissions. |
| `Final_Group_Report_DAP.docx` | Comprehensive academic report including methodology, dashboards, and insights. |
| `Dataset_references.txt` | Public links to all datasets used. |
| `MAIN_DASHBOARD 1.pdf` | Visual Tableau Dashboard displaying emissions, AQI, and per capita comparisons. |
| `x23289902.docx` | Contribution and workflow explanation for ETL and database integration. |

---

## 📊 Key Features

- 🚀 **Automated ETL Pipeline** using Dagster with PostgreSQL & MongoDB integration.
- 📉 **Air Quality vs Emissions Regression Model** – R² of 97.2%.
- 🌐 **Global Trend Analysis** across multiple pollutants (PM2.5, CO, NO₂, O₃).
- 📊 **Tableau Dashboard** showing top emitters, per capita breakdown, AQI patterns, and pollution hotspots.
- 🔁 **API Integration** with real-time data ingestion from KAPSARC API.

---

## 📈 Methodology

1. **Data Sources**:
   - [Global Fossil CO2 Emissions (2002–2022) – Kaggle](https://www.kaggle.com/datasets/thedevastator/global-fossil-co2-emissions-by-country-2002-2022)
   - [Global Air Pollution Dataset – Kaggle](https://www.kaggle.com/datasets/hasibalmuzdadid/global-air-pollution-dataset)
   - [Greenhouse Gas API – KAPSARC](https://datasource.kapsarc.org/explore/dataset/total-global-greenhouse-gas-emissions)

2. **Cleaning & Transformation**:
   - CSV data standardized, country codes unified.
   - API-based JSON flattened and stored in MongoDB.
   - PostgreSQL used for structured data storage and Tableau connectivity.

3. **Integration**:
   - Merged datasets by country and year.
   - Developed combined metrics like per capita emissions, pollutant-specific AQI, and emission types (CO₂, CH₄, N₂O).

4. **Analysis**:
   - Regression, visual trends, pollutant correlation analysis.

---

## 🔍 Key Insights (Based on Dashboard)

- **Top Emission Contributors**: China, USA, India.
- **Top Per Capita Emission Countries**: Qatar, Kuwait, UAE.
- **High AQI Countries**: India, Kuwait, Pakistan – high population + unregulated emissions.
- **Low AQI Countries**: Iceland, Argentina, Uruguay – managed industrial emissions with strong environmental regulations.
- **Per Capita ≠ Pollution**: High-income countries can manage air quality well (e.g., Luxembourg vs Qatar).

---

## 📤 How to Run Locally

### ✅ Prerequisites
- Python 3.8+
- PostgreSQL & MongoDB
- `pip install -r requirements.txt` (dependencies: dagster, pandas, sqlalchemy, requests, pycountry)

### 🔁 Run the ETL Pipeline
```bash
dagster dev  # Launch Dagster UI
```
Then run the following assets:
- `start_asset`
- `AQI_data_transform`
- `transform_clean_data_anually_emission`
- `api_data_clean_transform_load_to_mongo`
- `end_asset`

### 📊 Dashboard Setup
- Load final processed CSVs into Tableau.
- Connect Tableau to PostgreSQL (Database: `test`).
- Use the visuals in `MAIN_DASHBOARD 1.pdf` as template.

---

## 👨‍💻 Contributors

- **Utkarsh Satpute** – Data Analysis, Reporting, Visualization  
- **Tushar Gharpure** – ETL Pipeline, API Integration, MongoDB/PostgreSQL  
- **Pintoo Baghel** – Dataset Management, Cleaning, Tableau Dashboards  

---

## 📚 References

All research references, visuals, and citations are listed in the [Final Report](./Final_Group_Report_DAP.docx). Harvard format is used throughout.

---

## 📘 License

This project is for academic use only (National College of Ireland – MSc Data Analytics). Contact for collaboration or reuse.

---

## ✨ Tags

`#ETL` `#DataAnalytics` `#AirQuality` `#GreenhouseGas` `#Dagster` `#PostgreSQL` `#MongoDB` `#Tableau` `#Python` `#EnvironmentalAnalysis`