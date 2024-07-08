# news_collector_gdelt

This repository contains a data collection module designed to collect and process news articles from the GDELT (Global Database of Events, Language, and Tone) project. The primary goal of this module is to fetch news URLs, scrape the content, perform exploratory data analysis (EDA), and clean the collected data for further analysis or machine learning applications.

The news are collected from their Event Database 2.0. Raw files are collected from the following [link](http://data.gdeltproject.org/gdeltv2/masterfilelist.txt), which is updated with all GDELT collected news in english each 15 minutes.

More information about in their [GDELT 2.0 Event Database](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)

## Repository Structure

### Root Directory

- **Notebooks/**: Contains Jupyter notebooks for exploratory data analysis (EDA) on the collected news data.
- **data_cleaner/**: This directory includes scripts for cleaning the collected data.
- **gdelt_news_collector/**: Contains the main script for collecting news from GDELT and its Dockerfile for deployment.
- **lambda_web_scraper/**: Contains the AWS Lambda function for scraping news content from URLs.

### Notebooks/

- **eda_news_gdelt.ipynb**: A Jupyter notebook for performing exploratory data analysis on the collected news data.

### data_cleaner/

- **Dockerfile**: Dockerfile for setting up the data cleaner environment.
- **cleaner.py**: Script defining the cleaning function to process the news articles' bodies.
- **executor.py**: Main script to load, clean, and save the processed data.
- **loader.py**: Script for loading the collected CSV files from the S3 bucket.

### gdelt_news_collector/

- **Dockerfile**: Dockerfile for setting up the GDELT news collector environment.
- **news_collector.py**: Main script for collecting news URLs from GDELT, scraping content, and saving it to an S3 bucket.
- **requirements.txt**: List of Python dependencies required for the GDELT news collector.

### lambda_web_scraper/

- **lambda_scraper.py**: The AWS Lambda function code for scraping the content of news articles from URLs.
- **python-layer.zip**: AWS Lambda layer containing dependencies for the scraper.
- **test_lambda.txt**: Test cases or notes for the Lambda function.

## Getting Started

### Prerequisites

- Python 3.9 or higher
- AWS CLI configured with appropriate credentials
- Docker (optional, for containerized deployment)

### Setup

1. Clone the repository:
    ```sh
    git clone https://github.com/your_username/news_collector_gdelt.git
    cd news_collector_gdelt
    ```

2. Set up environment variables:
    - Create an `.env` file and fill in the required values:
      
    ```ini
      AWS_ACCESS_KEY_ID=your_access_key_id
      AWS_SECRET_ACCESS_KEY=your_secret_access_key
      AWS_REGION=your_aws_region
      S3_COLLECTOR_BUCKET_NAME=your_s3_collector_bucket_name
      LAMBDA_SCRAPER_FUNCTION_NAME=your_lambda_function_name
      S3_DESTINATION_BUCKET_NAME=your_s3_destination_bucket_name
      ```

3. Install the required dependencies:
    ```sh
    pip install -r gdelt_news_collector/requirements.txt
    ```

### Usage

#### Lambda Function Deployment

1. Navigate to the `lambda_web_scraper` directory:
    ```sh
    cd lambda_web_scraper
    ```

2. Deploy the Lambda function using AWS CLI:
    ```sh
    aws lambda update-function-code --function-name <your_lambda_function_name> --zip-file fileb://lambda_scraper.zip
    ```
You can also implement the **lambda_web_scraper/lambda_scraper.py** script in the **gdelt_news_collector/news_collector.py** script instead of deploying it to AWS lambda

#### Collect News from GDELT

1. Navigate to the `gdelt_news_collector` directory:
    ```sh
    cd gdelt_news_collector
    ```

2. Run the news collector script:
    ```sh
    python news_collector.py <start_date> <end_date> <concurrent_threads> <retry_skipped_dates>
    ```

#### Clean and Process Data

1. Navigate to the `data_cleaner` directory:
    ```sh
    cd data_cleaner
    ```

2. Run the executor script:
    ```sh
    python executor.py <number_of_files_to_process> <execution_mode>
    ```
