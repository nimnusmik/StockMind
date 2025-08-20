from datetime import datetime

#stocks = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
stocks = ['META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
output_dir = "/Users/sunminkim/Desktop/AIStages/StockMind/community/data"
logs_base_dir = "/Users/sunminkim/Desktop/AIStages/StockMind/community/logs"
cutoff_date = datetime(2025, 5, 1)

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

# Optional: Add proxy list (uncomment and configure if using proxies)
# proxy_list = [
#     {"server": "http://123.45.67.89:8080"},
#     {"server": "http://98.76.54.32:3128"},
# ]