
###  Project Overview**

This section is for anyone who wants to understand what your project does at a glance. It should be concise and easy to read.

#### **Project Title: Stock Trend Prediction with Yahoo Finance Community Sentiment**

---

#### **1. Project Summary**
This project aims to predict the daily stock price movement (up or down) of major tech companies by analyzing the sentiment of comments from the Yahoo Finance community. We'll build an automated data pipeline using **Apache Airflow** to handle the entire process, from data collection to model inference. The core idea is that collective public opinion, as expressed in online forums, can serve as a leading indicator of market sentiment, which in turn influences stock prices.

---

#### **2. Technologies Used**
* **Python**: The primary language for all scripts.
* **Crawling**: Scrapy, BeautifulSoup, or Selenium for collecting comment data.
* **Workflow Automation**: Apache Airflow for scheduling and orchestrating the data pipeline.
* **Database**: PostgreSQL or MySQL for storing raw and processed data.
* **Data Analysis**: Pandas, NumPy.
* **Machine Learning**: Scikit-learn, TensorFlow, or PyTorch for building the predictive model.

---

#### **3. Data Description**
The data is collected from the community section of Yahoo Finance for the following stocks: `AAPL`, `GOOG`, `META`, `TSLA`, `MSFT`, `AMZN`, `NVDA`, and `NFLX`. Each data point contains:
* **`time`**: The timestamp of the comment (e.g., `"12 Jul, 2025 11:48 PM"`).
* **`text`**: The raw text of the comment.
* **`stock_symbol`**: The ticker symbol for the stock.

---

#### **4. Current Status**
We've successfully developed the crawling script to extract the data and a separate script to migrate the collected data (stored as a CSV file) into a database. The next phase involves integrating these processes into an automated pipeline using Airflow and building the predictive model.
