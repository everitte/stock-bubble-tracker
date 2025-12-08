import yfinance as yf
import sqlite3
import pandas as pd
import pandas_datareader.data as web
from datetime import datetime, timedelta

DB_NAME = 'finance.db'

def get_db_connection():
    return sqlite3.connect(DB_NAME)

# ---------------------------------------------------------
# 1. BACKFILL DEVIATION (History Exists)
# ---------------------------------------------------------
def backfill_deviation():
    print("⏳ Backfilling Deviation (1 Year)...")
    # We fetch 2 years because we need the first year just to build the 200-day average
    df = yf.Ticker('^NDX').history(period="2y")
    
    # Calculate Indicators
    df['SMA_200'] = df['Close'].rolling(window=200).mean()
    df['Deviation'] = ((df['Close'] - df['SMA_200']) / df['SMA_200']) * 100
    
    # Trim to just the last year
    last_year = df.iloc[-365:].copy()
    
    conn = get_db_connection()
    count = 0
    for date, row in last_year.iterrows():
        if pd.notna(row['Deviation']): # Skip empty rows
            date_str = date.strftime('%Y-%m-%d')
            conn.execute('INSERT OR REPLACE INTO track_deviation VALUES (?,?,?,?)', 
                        (date_str, row['Close'], row['SMA_200'], row['Deviation']))
            count += 1
            
    conn.commit()
    conn.close()
    print(f"   ✅ Added {count} days of Deviation data.")

# ---------------------------------------------------------
# 2. BACKFILL LIQUIDITY (History Exists)
# ---------------------------------------------------------
def backfill_liquidity():
    print("⏳ Backfilling Liquidity (1 Year)...")
    start_date = datetime.now() - timedelta(days=365)
    
    try:
        # Fetch FRED data as Series
        rrp = web.DataReader('RRPONTSYD', 'fred', start_date)
        tga = web.DataReader('WTREGEN', 'fred', start_date)
        
        # Merge them into one DataFrame
        df = pd.DataFrame({'RRP': rrp['RRPONTSYD'], 'TGA': tga['WTREGEN']})
        df = df.dropna()
        
        conn = get_db_connection()
        count = 0
        for date, row in df.iterrows():
            date_str = date.strftime('%Y-%m-%d')
            conn.execute('INSERT OR REPLACE INTO track_liquidity VALUES (?,?,?)', 
                        (date_str, row['RRP'], row['TGA']))
            count += 1
            
        conn.commit()
        conn.close()
        print(f"   ✅ Added {count} days of Liquidity data.")
    except Exception as e:
        print(f"   ❌ Liquidity Backfill Failed: {e}")

# ---------------------------------------------------------
# 3. BACKFILL IPO HEAT (History Exists)
# ---------------------------------------------------------
def backfill_ipo():
    print("⏳ Backfilling IPO Heat (1 Year)...")
    df = yf.Ticker("IPO").history(period="1y")
    
    # Calculate Rolling Average Volume (using 20-day average as baseline)
    df['Avg_Vol'] = df['Volume'].rolling(window=20).mean()
    df['Heat_Ratio'] = df['Volume'] / df['Avg_Vol']
    
    conn = get_db_connection()
    count = 0
    for date, row in df.iterrows():
        if pd.notna(row['Heat_Ratio']):
            date_str = date.strftime('%Y-%m-%d')
            conn.execute('INSERT OR REPLACE INTO track_ipo_heat VALUES (?,?,?)', 
                        (date_str, row['Close'], row['Heat_Ratio']))
            count += 1
            
    conn.commit()
    conn.close()
    print(f"   ✅ Added {count} days of IPO data.")

if __name__ == "__main__":
    backfill_deviation()
    backfill_liquidity()
    backfill_ipo()
    print("\n⚠️ Note: CNN Fear & Greed does not offer history. That chart will build up daily starting now.")
