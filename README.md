# Automated ETL Pipeline and Data Visualization

This project implements an automated ETL (Extract, Transform, Load) pipeline and an interactive data visualization dashboard. The pipeline is developed using Apache Airflow for orchestration, while data visualization is achieved using Plotly. The project focuses on processing and visualizing stock data sourced from Alpha Vantage API and Yahoo Finance.

## Project Overview

### Features

- **Data Extraction**
  - Extract daily stock data from Alpha Vantage API.
  - Scrape financial data from Yahoo Finance using BeautifulSoup.
  
- **Data Transformation**
  - Transform daily stock data into weekly, monthly, and yearly aggregates.
  - Enrich datasets with metadata sourced from Yahoo Finance.
  
- **Data Integration**
  - Store cleaned and enriched datasets in MongoDB Atlas for efficient storage and real-time access.
  
- **Automation**
  - Automate the complete ETL pipeline using Apache Airflow.
  
- **Data Visualization**
  - Develop an interactive dashboard using Plotly to visualize aggregated stock data alongside company metadata.

## Skills Utilized

- Apache Airflow
- Ubuntu
- Plotly
- Pipelines
- Data Engineering
- Data Analysis
- Data Visualization

## Installation and Setup

### Prerequisites

- Python 3.7+
- MongoDB Atlas account
- Alpha Vantage API key
- Ubuntu (or any Unix-based system)
- Apache Airflow
