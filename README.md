# US Stock Market Aggregation with Polygon.io and QuestDB

This project implements a robust solution for retrieving and storing daily aggregation data of the US stock market for over 2,000 tickers. By leveraging the capabilities of Polygon.io, QuestDB, and Python, the project efficiently updates and maintains stock data on each execution.

## Features
- Retrieve daily aggregation data for over 2,000 stock tickers from the US stock market.
- Store the aggregated data in a QuestDB table for optimized querying and analysis.
- Automatically update data on each execution to ensure consistency and reliability.
- Built with Python, utilizing libraries like `pandas`, `SQLAlchemy`, and the `polygon.io` client for seamless integration.

## Tech Stack
- **Programming Language**: Python
- **Database**: QuestDB
- **Data Source**: [Polygon.io](https://polygon.io/)
- **Key Libraries**: 
  - `pandas` for data manipulation and analysis
  - `SQLAlchemy` for database interaction
  - `polygon.io` client for API integration

## Setup Instructions

### Prerequisites
- Python 3.7 or higher
- QuestDB installed and running ([QuestDB Documentation](https://questdb.io/docs/))
- Polygon.io API key ([Get an API Key](https://polygon.io/))

