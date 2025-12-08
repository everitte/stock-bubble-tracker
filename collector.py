import yfinance as yf
import sqlite3
import pandas as pd
import pandas_datareader.data as web
import requests
from datetime import datetime, timedelta

# CONFIGURATION
DB_NAME = 'finance.db'

# --- SHARED: Database Helper ---
def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_tables():
    """Creates the 4 separate tables if they don't exist."""
    conn = get_db_connection()
    c = conn.cursor()
    
    # Table 1: Deviation
    c.execute('''CREATE TABLE IF NOT EXISTS track_deviation (
                 date TEXT PRIMARY KEY, price REAL, sma_200 REAL, deviation_pct REAL)''')
    
    # Table 2: Liquidity
    c.execute('''CREATE TABLE IF NOT EXISTS track_liquidity (
                 date TEXT PRIMARY KEY, rrp_billions REAL, tga_billions REAL)''')
    
    # Table 3: Sentiment
    c.execute('''CREATE TABLE IF NOT EXISTS track_sentiment (
                 date TEXT PRIMARY KEY, fear_greed_score REAL, rating TEXT)''')
                 
    # Table 4: IPO Heat
    c.execute('''CREATE TABLE IF NOT EXISTS track_ipo_heat (
                 date TEXT PRIMARY KEY, ipo_etf_price REAL, vol_heat_ratio REAL)''')
                 
    conn.commit()
    conn.close()

# --- MODULE 1: DEVIATION ---
def run_deviation():
    print("1️⃣  Processing Deviation...")
    try:
        df = yf.Ticker('^NDX').history(period="1y")
        curr = df['Close'].iloc[-1]
        sma = df['Close'].rolling(window=200).mean().iloc[-1]
        dev = ((curr - sma) / sma) * 100
        
        today = datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        conn.execute('INSERT OR REPLACE INTO track_deviation VALUES (?,?,?,?)', 
                    (today, curr, sma, dev))
        conn.commit()
        conn.close()
        print(f"   ✅ Saved Deviation: {dev:.2f}%")
    except Exception as e:
        print(f"   ❌ Failed: {e}")

# --- MODULE 2: LIQUIDITY ---
def run_liquidity():
    print("2️⃣  Processing Liquidity...")
    try:
        start = datetime.now() - timedelta(days=5)
        rrp = web.DataReader('RRPONTSYD', 'fred', start).iloc[-1].item()
        tga = web.DataReader('WTREGEN', 'fred', start).iloc[-1].item()
        
        today = datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        conn.execute('INSERT OR REPLACE INTO track_liquidity VALUES (?,?,?)', 
                    (today, rrp, tga))
        conn.commit()
        conn.close()
        print(f"   ✅ Saved Liquidity: RRP ${rrp}B")
    except Exception as e:
        print(f"   ❌ Failed: {e}")

# --- MODULE 3: SENTIMENT ---
def run_sentiment():
    print("3️⃣  Processing Sentiment...")
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        data = r.json()
        score = data['fear_and_greed']['score']
        rating = data['fear_and_greed']['rating']
        
        today = datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        conn.execute('INSERT OR REPLACE INTO track_sentiment VALUES (?,?,?)', 
                    (today, score, rating))
        conn.commit()
        conn.close()
        print(f"   ✅ Saved Sentiment: {score:.0f} ({rating})")
    except Exception as e:
        print(f"   ❌ Failed: {e}")

# --- MODULE 4: IPO HEAT ---
def run_ipo_heat():
    print("4️⃣  Processing IPO Heat...")
    try:
        hist = yf.Ticker("IPO").history(period="5d")
        curr = hist['Close'].iloc[-1]
        vol_ratio = hist['Volume'].iloc[-1] / hist['Volume'].mean()
        
        today = datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        conn.execute('INSERT OR REPLACE INTO track_ipo_heat VALUES (?,?,?)', 
                    (today, curr, vol_ratio))
        conn.commit()
        conn.close()
        print(f"   ✅ Saved IPO Heat: {vol_ratio:.2f}x")
    except Exception as e:
        print(f"   ❌ Failed: {e}")

if __name__ == "__main__":
    init_tables() # Ensure tables exist
    run_deviation()
    run_liquidity()
    run_sentiment()
    run_ipo_heat()