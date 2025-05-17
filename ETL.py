from dagster import asset, Output, SourceAsset
import pandas as pd
from pymongo import MongoClient
import requests
import pycountry
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os

# Database configurations
DATABASE_URI = os.getenv('DATABASE_URI', 'postgresql+psycopg2://postgres:Admin@localhost:5432/Test')
MONGO_URI = os.getenv(
    'MONGO_URI',
    f'mongodb+srv://{quote_plus("tushargharpure9")}:{quote_plus("WRJIzpcGpNwGLR89")}@cluster0.iqn2l.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
)
API_KEY = os.getenv('API_KEY', "b6aef8e4e848f3088b0adabc86cdf72d2618862699a320896f34d6da")
BASE_URL = os.getenv('BASE_URL', "https://datasource.kapsarc.org/api/explore/v2.1/catalog/datasets/total-global-greenhouse-gas-emissions/records")

# Define PostgreSQL as a SourceAsset
PostgreSQL = SourceAsset(key="PostgreSQL")


# Helper: Get country code from name
def get_country_code(country_name):
    try:
        return pycountry.countries.lookup(country_name).alpha_3
    except LookupError:
        return None


@asset
def start_asset(context):
    """Starting point for the pipeline."""
    context.log.info("Pipeline started.")
    return {"status": "started"}


@asset
def AQI_data_transform(context, start_asset):
    try:
        file_path = os.getenv("AQI_DATA_PATH", r"C:\Users\utkar\global AP dataset.csv")
        df = pd.read_csv(file_path)

        df['Country_Code'] = df['Country'].apply(get_country_code)
        df.drop(columns=['City'], errors='ignore', inplace=True)
        df.sort_values(by=['Country'], ascending=True, inplace=True)

        record_count = len(df)
        context.log.info(f"AQI data processed with {record_count} records.")
        return Output(value=df, metadata={"record_count": record_count})
    except Exception as e:
        context.log.error(f"Error in AQI_data_transform: {e}")
        return Output(value=None, metadata={"error": str(e)})


@asset
def AQI_Load_to_PostgreSQL(context, AQI_data_transform):
    try:
        df = AQI_data_transform.value

        table_name = 'Aqi'
        engine = create_engine(DATABASE_URI)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)

        record_count = len(df)
        context.log.info(f"AQI data successfully stored in table '{table_name}' with {record_count} records.")
        return Output(value=df, metadata={"record_count": record_count})
    except Exception as e:
        context.log.error(f"Error in AQI_Load_to_PostgreSQL asset: {e}")
        return Output(value=None, metadata={"error": str(e)})


@asset
def transform_clean_data_anually_emission(context, start_asset):
    try:
        file_path = os.getenv("EMISSION_DATA_PATH", r"C:\Users\utkar\anually_emission.csv")
        df = pd.read_csv(file_path)

        df_filtered = df[df['Year'] >= 2000]
        df_filtered.sort_values(by=['Country', 'Year'], inplace=True)
        df_filtered.drop(columns=['Cement', 'Flaring', 'Other'], errors='ignore', inplace=True)
        df_filtered.rename(columns={'ISO 3166-1 alpha-3': 'Country_codes'}, inplace=True)

        record_count = len(df_filtered)
        context.log.info(f"Emission data filtered with {record_count} records.")
        return Output(value=df_filtered, metadata={"record_count": record_count})
    except Exception as e:
        context.log.error(f"Error in transform_clean_data_anually_emission: {e}")
        return Output(value=None, metadata={"error": str(e)})


@asset
def store_annual_emission_data(context, transform_clean_data_anually_emission):
    try:
        df_filtered = transform_clean_data_anually_emission.value

        table_name = 'Emission'
        engine = create_engine(DATABASE_URI)
        df_filtered.to_sql(table_name, con=engine, if_exists='replace', index=False)

        record_count = len(df_filtered)
        context.log.info(f"Emission data successfully stored in table '{table_name}' with {record_count} records.")
        return Output(value=df_filtered, metadata={"record_count": record_count})
    except Exception as e:
        context.log.error(f"Error in store_annual_emission_data: {e}")
        return Output(value=None, metadata={"error": str(e)})


@asset
def api_data_clean_transform_load_to_mongo(context, start_asset):
    try:
        def fetch_all_data():
            all_data = []
            limit = 100
            offset = 0

            while True:
                url = f"{BASE_URL}?limit={limit}&offset={offset}&apikey={API_KEY}"
                response = requests.get(url)
                if response.status_code != 200:
                    context.log.error(f"Error fetching API data: {response.status_code}, {response.text}")
                    break

                data = response.json().get('results', [])
                all_data.extend(data)

                if not data:
                    break
                offset += limit

            return all_data

        def transform_data(data):
            df = pd.DataFrame(data)
            if 'world_region' in df.columns:
                df.drop(columns=['world_region'], errors='ignore', inplace=True)
            if 'year' in df.columns:
                df['year'] = pd.to_numeric(df['year'], errors='coerce')
                df = df[(df['year'] >= 2000) & (df['year'] <= 2021)]
            if 'country' in df.columns:
                df['country_code'] = df['country'].apply(get_country_code)
            return df

        data = fetch_all_data()
        if not data:
            context.log.warning("No data retrieved from API.")
            return Output(value=None, metadata={"error": "No data retrieved"})

        df = transform_data(data)

        client = MongoClient(MONGO_URI)
        db = client['tushardb']
        collection = db['greenhouse']

        # Ensure no duplicates in MongoDB
        collection.delete_many({})
        collection.insert_many(df.to_dict(orient='records'))

        # Save the transformed data as a CSV file in the local download path
        download_path = 'C:\Users\utkar\Downloads\transformed_emissions_data.csv'
        transform_data.to_csv(download_path, index=False)
        context.log.info(f"Transformed data saved as CSV at: {download_path}")


        context.log.info(f"Inserted {len(df)} records into MongoDB.")
        return Output(value=df, metadata={"record_count": len(df)})
    except Exception as e:
        context.log.error(f"Error in api_data_clean_transform_load_to_mongo: {e}")
        return Output(value=None, metadata={"error": str(e)})


@asset
def end_asset(context, api_data_clean_transform_load_to_mongo, PostgreSQL, store_annual_emission_data,AQI_Load_to_PostgreSQL):
    """Ending point for the pipeline."""
    try:
        context.log.info("End asset executed successfully.")
        return Output(value={"status": "Success"}, metadata={
            "api_data_records": len(api_data_clean_transform_load_to_mongo.value) if api_data_clean_transform_load_to_mongo.value else 0,
            "postgresql_records": PostgreSQL.metadata.get("record_count", 0),
            "store_emission_records": store_annual_emission_data.metadata.get("record_count", 0),
        })
    except Exception as e:
        context.log.error(f"Error in end_asset: {e}")
        return Output(value={"status": "Failed"}, metadata={"error": str(e)})
