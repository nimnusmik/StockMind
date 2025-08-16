import numpy as np
import pandas as pd
import mplfinance as mpf
import os
import random
from tqdm import tqdm
from datetime import datetime, timedelta

def create_cup_handle_chart(num):
    base_date = datetime.today()
    cup_days = 12
    handle_days = 5
    breakout_days = 3
    total_days = cup_days + handle_days + breakout_days
    dates = [base_date - timedelta(days=i) for i in reversed(range(total_days))]

    start_price = 100
    open_prices, high_prices, low_prices, close_prices = [], [], [], []
    current_price = start_price

    for i in range(total_days):
        if i < cup_days:  # Cup
            x = (i - (cup_days/2)) / (cup_days/2)
            cup_shape = - (x ** 2) * random.uniform(0.2, 0.4)
            body = cup_shape + np.random.uniform(-0.1, 0.1)
            wick = np.random.uniform(0.5, 1.0)

        elif cup_days <= i < cup_days + handle_days:  # Handle
            body = np.random.uniform(-0.5, -0.1)
            wick = np.random.uniform(0.3, 0.8)

        else:  # Breakout
            body = np.random.uniform(2.0, 3.5)
            wick = np.random.uniform(0.5, 1.0)

        open_p = current_price
        close_p = open_p + body
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

    return save_cup_handle_file(num, df)

def save_cup_handle_file(num, df):
    output_dir = '../results/short_pattern/cup_handle_pattern'
    os.makedirs(output_dir, exist_ok=True)

    file_name = f'cup_handle_{(num+1):04d}.png'
    file_path = os.path.join(output_dir, file_name)

    mpf.plot(
        df,
        type='candle',
        style='charles',
        title='Cup and Handle Pattern',
        savefig=file_path
    )


def start():
    for num in tqdm(range(1000), desc='Generating cup and handle Charts'):
        create_cup_handle_chart(num)
    return ('Success!')
