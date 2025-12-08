import streamlit as st
import sqlite3
import pandas as pd

# PAGE CONFIGURATION
st.set_page_config(page_title="Market Risk Dashboard", layout="wide")
st.title("📉 Market Risk Dashboard")

# HELPER: Load data from DB
def load_data(table_name):
    conn = sqlite3.connect('finance.db')
    # Read all data and sort by date
    df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY date ASC", conn)
    conn.close()
    return df

# REFRESH BUTTON
if st.button('🔄 Refresh Data'):
    st.rerun()

# ---------------------------------------------------------
# ROW 1: VALUATION & SENTIMENT
# ---------------------------------------------------------
col1, col2 = st.columns(2)

# --- PANEL 1: NASDAQ DEVIATION ---
with col1:
    st.subheader("1. Price Deviation (Nasdaq 100)")
    df_dev = load_data("track_deviation")
    
    if not df_dev.empty:
        # Get latest values
        latest = df_dev.iloc[-1]
        val = latest['deviation_pct']
        
        # dynamic color logic
        color = "normal"
        if val > 15: color = "inverse" # Red in streamlit
        
        st.metric(label="Current Deviation", value=f"{val:.2f}%", delta=f"Price: ${latest['price']:,.0f}", delta_color=color)
        
        # Draw Chart
        st.line_chart(df_dev, x='date', y='deviation_pct')
        st.caption("Target: Deviation should be close to 0%. >15% is 'Overextended'.")
    else:
        st.warning("No data yet. Run collector.py")

# --- PANEL 2: MARKET SENTIMENT ---
with col2:
    st.subheader("2. Market Sentiment (Fear & Greed)")
    df_sent = load_data("track_sentiment")
    
    if not df_sent.empty:
        latest = df_sent.iloc[-1]
        val = latest['fear_greed_score']
        
        st.metric(label="Fear & Greed Index", value=f"{val:.0f}/100", delta=latest['rating'])
        
        st.line_chart(df_sent, x='date', y='fear_greed_score')
        st.caption("0-25: Extreme Fear (Buy) | 75-100: Extreme Greed (Sell)")
    else:
        st.warning("No data yet.")

st.divider()

# ---------------------------------------------------------
# ROW 2: LIQUIDITY & IPOs
# ---------------------------------------------------------
col3, col4 = st.columns(2)

# --- PANEL 3: LIQUIDITY (FED) ---
with col3:
    st.subheader("3. Fed Liquidity Plumbing")
    df_liq = load_data("track_liquidity")
    
    if not df_liq.empty:
        latest = df_liq.iloc[-1]
        st.metric(label="Reverse Repo (Cash Drain)", value=f"${latest['rrp_billions']:.0f}B")
        
        # Compare RRP vs TGA
        st.area_chart(df_liq, x='date', y=['rrp_billions', 'tga_billions'])
        st.caption("Lower RRP & TGA is generally better for liquidity.")
    else:
        st.warning("No data yet.")

# --- PANEL 4: IPO HEAT ---
with col4:
    st.subheader("4. IPO Market Heat")
    df_ipo = load_data("track_ipo_heat")
    
    if not df_ipo.empty:
        latest = df_ipo.iloc[-1]
        val = latest['vol_heat_ratio']
        
        st.metric(label="IPO Volume Ratio", value=f"{val:.2f}x", delta="Vs 5-day Avg")
        
        st.bar_chart(df_ipo, x='date', y='vol_heat_ratio')
        st.caption("Spikes > 2.0x indicate high speculative activity.")
    else:
        st.warning("No data yet.")