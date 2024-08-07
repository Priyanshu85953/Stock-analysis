import ccxt
import pandas as pd
from pytz import timezone
import numpy as np

def fetch_minute_data(exchange, symbol, start_time, end_time, limit=1000):
    # Initialize an empty list to store OHLCV data
    ohlcv_data = []
    rows_to_save = []  # Initialize a list to store rows to be saved

    # Calculate the number of requests needed to fetch all data points
    num_requests = (end_time - start_time) // (limit * 60000) + 1  # 60000 milliseconds per 1 minute

    # Ensure start_time ends in 0 (e.g., 12:30, 12:40)
    start_time -= start_time % 600000  # Modulus with 10 minutes in milliseconds (600000 ms)

    # Fetch OHLCV data in chunks
    for i in range(num_requests):
        # Calculate the start time for the current request
        current_start_time = start_time + i * (limit * 60000)

        # Fetch OHLCV data for the current chunk
        ohlcv = exchange.fetch_ohlcv(symbol, '1m', since=current_start_time, limit=limit)
        
        # Append the fetched data to the list
        ohlcv_data.extend(ohlcv)
        
        # Break if fewer data points are returned than the limit
        if len(ohlcv) < limit:
            break
    
    # Create a DataFrame from the fetched data
    df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    # Convert timestamps to datetime and localize to UTC
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC')

    # Convert timestamps to Indian Standard Time (IST)
    ist_timezone = timezone('Asia/Kolkata')
    df['timestamp'] = df['timestamp'].dt.tz_convert(ist_timezone)
    
    # Add a column for the day of the week
    df['day_of_week'] = df['timestamp'].dt.day_name()

    # Collect rows where open value is lesser than the close value of the 10th interval and start time ends in 0
    for i in range(len(df) - 10):
        current_time = df['timestamp'][i]
        if current_time.minute % 10 == 0:
            if df['open'][i] > df['close'][i + 9]:
                rows_to_save.append(df.iloc[i])
    
    # Create a DataFrame from collected rows
    saved_df = pd.DataFrame(rows_to_save)

    # Save DataFrame to CSV
    saved_df.to_csv('saved_rows.csv', index=False)
    
    return df

def find_repeated_times(saved_csv):
    # Load the saved data
    df = pd.read_csv(saved_csv, parse_dates=['timestamp'])

    # Extract hour and minute from the timestamp
    df['time'] = df['timestamp'].dt.strftime('%H:%M')

    # Filter times ending with 0
    df = df[df['time'].str.endswith('0')]

    # Count occurrences of each time
    time_counts = df['time'].value_counts()

    # Save the time counts to CSV
    time_counts_df = time_counts.reset_index()
    time_counts_df.columns = ['time', 'count']
    time_counts_df.to_csv('time_counts.csv', index=False)

    return time_counts

# Example usage:
exchange = ccxt.binance()  # Assuming you want to use Binance exchange
symbol = 'BTC/USDT'  # Bitcoin to USDT pair
start_time = exchange.parse8601('2024-06-01T00:00:00Z')  # Example start time
end_time = exchange.parse8601('2024-06-18T00:00:00Z')  # Example end time
minute_data = fetch_minute_data(exchange, symbol, start_time, end_time)

# Save full DataFrame to CSV
minute_data.to_csv('minute_bitcoin_price_data_IST.csv', index=False)

# Find repeated times in the saved data
time_counts = find_repeated_times('saved_rows.csv')
print("Times and their occurrences for timestamps ending in 0:")
print(time_counts)
