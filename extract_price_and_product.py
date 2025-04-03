import pandas as pd
import requests
from datetime import timedelta, datetime, tzinfo, timezone
import duckdb
import os
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.getenv('PROD_EXTRACT_PATH')),
        logging.StreamHandler()
    ]
)

def fetch_prices(product_id):
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
    # supplementing utcnow with tmz.utc, not sure correctness
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=15)
    params = {
        "granularity": 86400,
        "start": start_date.isoformat() + "Z", 
        "end": end_date.isoformat() + "Z"
    }
    logging.info(f"Fetching {product_id} data between {start_date} and {end_date}..")
    r = requests.get(url, params=params)
    if r.status_code == 200:
        data = r.json()
        df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df.sort_values("time", inplace=True)
        df["product_id"] = product_id
        df["last_updated"] = end_date
        return df
    elif r.status_code == 404:
        logging.warning(f"Data for {product_id} could not be found.")
        return pd.DataFrame()
    else:
        logging.error(f"Failed to fetch data for {product_id}: {r.status_code}, {r.text}")
        raise Exception(f"Failed to fetch data for {product_id}: {r.status_code}, {r.text}")
          
def fetch_product():
    url = "https://api.exchange.coinbase.com/products"
    params = {
        "granularity": 86400,
    }
    r = requests.get(url, params=params)
    if r.status_code == 200:
        data = r.json()
        df = pd.DataFrame(data, columns=[
                'id',
                'base_currency',
                'quote_currency',
                'quote_increment',
                'base_increment',
                'display_name',
                'min_market_funds',
                'margin_enabled',
                'post_only',
                'limit_only',
                'cancel_only',
                'status',
                'status_message',
                'trading_disabled',
                'fx_stablecoin',
                'max_slippage_percentage',
                'auction_mode',
                'high_bid_limit_percentage'        
                ])
        df['last_updated'] = datetime.now(timezone.utc)
        logging.info("Fetched product data")
        return df
    else:
        logging.error(f"Failed to fetch data: {r.status_code}, {r.text}")
        raise Exception(f"Failed to fetch data: {r.status_code}, {r.text}")

def save_prices(df, product, db_file=DATABASE_FILE):

    table_name = f'"{TABLE_NAME_TEMPLATE}"'
    temp_table = f'"{TEMP_TABLE_TEMPLATE}"'

    required_columns = ["time", "low", "high", "open", "close", "volume","product_id","last_updated"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    # add more validation against data being loaded
    if missing_columns:
        logging.error(f"DataFrame is missing required columns: {missing_columns}")
        raise ValueError(f"DataFrame is missing required columns: {missing_columns}")
    
    df = df[required_columns]
    df.reset_index(drop=True, inplace=True)
    conn = duckdb.connect(database=db_file)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            time TIMESTAMP,
            low DOUBLE,
            high DOUBLE,
            open DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            product_id VARCHAR,
            last_updated TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {temp_table} (
            time TIMESTAMP,
            low DOUBLE,
            high DOUBLE,
            open DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            product_id VARCHAR,
            last_updated TIMESTAMP
        )
    """)
    if not df.empty:
        conn.execute(f"INSERT INTO {temp_table} SELECT * FROM df")
        conn.execute(f"""
            UPDATE {table_name}
            SET
                low = {temp_table}.low,
                high = {temp_table}.high,
                open = {temp_table}.open,
                close = {temp_table}.close,    
                volume = {temp_table}.volume,
                product_id = {temp_table}.product_id,     
                last_updated = {temp_table}.last_updated
            FROM {temp_table} 
            WHERE {table_name}.time = {temp_table}.time AND {table_name}.product_id = {temp_table}.product_id    
                    """)
        conn.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_table}
            WHERE NOT EXISTS (
            SELECT 1 FROM {table_name}
            WHERE {table_name}.time = {temp_table}.time AND {table_name}.product_id = {temp_table}.product_id
            )
            """)
        logging.info(f"Data merged into {table_name}.")
    else:
        logging.info("No data was merged.")
    conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_time ON {table_name} (time, product_id)")
    conn.execute(f"DROP TABLE {temp_table}")
    conn.close()    


def save_product(df, db_file=DATABASE_FILE, table_name=PRODUCT_TABLE):

    required_columns = [
                'id','base_currency','quote_currency','quote_increment','base_increment',
                'display_name','min_market_funds','margin_enabled','post_only','limit_only',
                'cancel_only','status','status_message','trading_disabled',
                'fx_stablecoin','max_slippage_percentage','auction_mode','high_bid_limit_percentage','last_updated'
                ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"DataFrame is missing required columns: {missing_columns}")
        raise ValueError(f"DataFrame is missing required columns: {missing_columns}")
    df = df[required_columns]
    df.reset_index(drop=True, inplace=True)
    conn = duckdb.connect(database=db_file)
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} (
            id VARCHAR,
            base_currency VARCHAR,
            quote_currency VARCHAR,
            quote_increment DECIMAL(20,10),
            base_increment DECIMAL(20,10),
            display_name VARCHAR,
            min_market_funds DECIMAL(20,10),
            margin_enabled BOOLEAN,
            post_only BOOLEAN,
            limit_only BOOLEAN,
            cancel_only BOOLEAN,
            status VARCHAR,
            status_message VARCHAR,
            trading_disabled BOOLEAN,
            fx_stablecoin BOOLEAN,
            max_slippage_percentage DECIMAL(10,8),
            auction_mode BOOLEAN,
            high_bid_limit_percentage VARCHAR,
            last_updated TIMESTAMP WITH TIME ZONE  
        )
    """)
    if not df.empty:
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        logging.info(f"Data merged into {table_name}.")
    else:
        logging.info("No data was merged.")
    conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_id ON {table_name} (id)")
    conn.close()    

def fetch_and_save_all():
    now = datetime.now(timezone.utc)
    logging.info("Starting extraction @ {now}..")
    product_df = fetch_product()
    save_product(product_df)
    
    for product_id in product_df["id"]:
        product = product_id.replace("-","_")
        logging.info(f"Processing product: {product}")
        try:
            price_df = fetch_prices(product_id)
            save_prices(price_df, product)
        except Exception as e:
            logging.exception(f"Error processing {product_id}: {e}")

DATABASE_FILE = os.getenv('DB_PATH')
TABLE_NAME_TEMPLATE = f"price_data"
PRODUCT_TABLE = "products"
TEMP_TABLE_TEMPLATE = f"temp_price_data"

if __name__ == "__main__":
    fetch_and_save_all()