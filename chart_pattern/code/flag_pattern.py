import numpy as np
import pandas as pd
import mplfinance as mpf
import os
import random
from tqdm import tqdm
from datetime import datetime, timedelta

def create_flag_chart(num, bullish=True):
    base_date = datetime.today()
    total_days = 7 + 5 + 3
    dates = [base_date - timedelta(days=i) for i in reversed(range(total_days))]

    start_price = 100
    open_prices, high_prices, low_prices, close_prices = [], [], [], []
    current_price = start_price

    direction = 1 if bullish else -1
    # Pole 구간 (급격한 상승/하락)
    
    for i in range(15):
        if i < 7:
            body = np.random.uniform(1.5, 3.0)
            wick = np.random.uniform(0.5, 1.5)

        elif 7 <= i < 12:
            body = np.random.uniform(-1.0, 1.0)
            wick = np.random.uniform(0.3, 0.8)

        else:
            body = np.random.uniform(2.0, 3.5)
            wick = np.random.uniform(0.5, 1.0)

        open_p = current_price
        close_p = open_p + body * direction
        high_p = max(open_p, close_p) + wick
        low_p = min(open_p, close_p) - wick

        open_prices.append(open_p)
        close_prices.append(close_p)
        high_prices.append(high_p)
        low_prices.append(low_p)

        current_price = close_p

    df = pd.DataFrame({
        "Open": open_prices,
        "High": high_prices,
        "Low": low_prices,
        "Close": close_prices
    }, index=pd.DatetimeIndex(dates))

    return save_file(num, df, bullish)

def save_file(num, df, bullish):
    base_dir = '../results/short_pattern/flag_pattern'
    label = 'bullish' if bullish else 'bearish'
    output_dir = os.path.join(base_dir, label)
    os.makedirs(output_dir, exist_ok=True)

    file_name = f'{label}_flag_{(num+1):04d}.png'
    file_path = os.path.join(output_dir, file_name)

    mpf.plot(
        df,
        type='candle',
        style='charles',
        title=f'{label.capitalize()} flag',
        savefig=file_path
    )


def start():
    for num in tqdm(range(1000), desc='Generating Flag Charts'):
        create_flag_chart(num, bullish=True)
        create_flag_chart(num, bullish=False)
    return ('Success!')
