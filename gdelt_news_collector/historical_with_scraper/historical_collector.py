import pandas as pd
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from dotenv import load_dotenv
import concurrent.futures
import time
import logging
from lambda_scraper import parallel_scraping
from cleaner_saver import CleanerSaver

#Load the environment
load_dotenv()

#Configure the logger
logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Define the log format
    handlers=[logging.StreamHandler()]  # Ensure logs are sent to stdout
)

logger = logging.getLogger(__name__)

#Get the credentials from the environment
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

#The bucket to store the news
s3_bucket_name = os.getenv('S3_COLLECTOR_BUCKET_NAME')

# Get script parameters from environment variables
start_date = os.getenv('START_DATE')
end_date = os.getenv('END_DATE')
concurrent_threads = int(os.getenv('CONCURRENT_THREADS', 5))
retry_skipped_dates_arg = os.getenv('RETRY_SKIPPED_DATES', 'no').lower()
timeout = int(os.getenv("SCRAPER_TIMEOUT", 5))
scraper_max_workers = int(os.getenv('SCRAPER_MAX_WORKERS', 5))

#Take count of the dates skipped, either by error or by max_retries in the lambda fucntion call
skipped_dates = []
url_col_idx = 60

def parallel_apply(df, func, max_workers=4):
    """
    Applies a function to all rows of the 'body' column in the DataFrame in parallel.

    Parameters:
    - df: pandas DataFrame, The DataFrame to apply the function to.
    - func: callable, The function to apply to each element of the 'body' column.
    - max_workers: int, The maximum number of threads to use.

    Returns:
    - df: pandas DataFrame, The DataFrame with the applied function.
    """
    # The resulting column after applying the function
    result_series = pd.Series(index=df.index, dtype=object)

    def apply_function(row_index, row_value):
        try:
            return func(row_value)
        except Exception as e:
            logger.error(f"Error applying function to row {row_index}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Map each row index and value to the executor
        futures = {
            executor.submit(apply_function, idx, val): idx
            for idx, val in df['body'].items()
        }

        for future in concurrent.futures.as_completed(futures):
            row_index = futures[future]
            try:
                result = future.result()
                result_series.at[row_index] = result
            except Exception as e:
                logger.error(f"Error in future for row {row_index}: {e}")
                result_series.at[row_index] = None

    df['body'] = result_series
    df.dropna(subset=['body'], inplace=True)
    return df

def join_dfs_clean_and_save(accumulated_results, cleaner_saver):
    #Clean data
    '''
    for df in accumulated_results:
        df['body'] = df['body'].apply(cleaner_saver.clean_text)
        df.dropna(subset=['body'], inplace=True)
    '''
    max_workers = 8  # You can adjust this based on your CPU cores
    cleaned_dataframes = []
    for df_to_clean in accumulated_results:
        cleaned_df = parallel_apply(df_to_clean, cleaner_saver.clean_text, max_workers=max_workers)
        cleaned_dataframes.append(cleaned_df)
                    
    #Create a df appending every DF in the accumulated results list
    combined_df = pd.concat([d.transpose() for d in cleaned_dataframes], axis=1, ignore_index=True).T

    #Create filename for parquet file
    start_date = pd.to_datetime(combined_df['date']).min().strftime('%Y%m%d%H%M%S')
    end_date = pd.to_datetime(combined_df['date']).max().strftime('%Y%m%d%H%M%S')
    parquet_file_name = f"news_{start_date}_to_{end_date}.parquet"

    #Call the CS to save to parquet
    cleaner_saver.save_to_parquet(combined_df, s3_bucket_name, file_name=parquet_file_name)

    #Inform about the upload and the current date we have reached scraping
    logger.info(f"File {pd.to_datetime(combined_df['date']).max().strftime('%Y-%-m %H:%M:%S')} uploaded to S3!\nCcheckpoint Date: {end_date}")
    

def scrape_into_df(url_list, date_of_file):
    """
    Scrapes the provided URLs and saves the results to the S3 bucket provided in the .env file.

    Parameters:
    url_list (list of str): A list of URLs to scrape.
    date_of_file (datetime): The date of the urls, provided by GDELT in the file collected.

    Returns:
    None
    """
    try:
        # Scrape the URLs
        results = parallel_scraping(url_list, max_workers=scraper_max_workers, timeout=timeout)
            
        # Process results
        results_df = pd.DataFrame([{"url": k, "title": v[0], "body": v[1]} for d in results for k, v in d.items() if v is not None])
        
        #Add date as a column
        results_df["date"] = date_of_file.strftime("%Y-%m-%d %H:%M:%S")

        #Drop the rows with NaN values
        df_for_s3 = results_df.dropna()

        #Return the DF
        return df_for_s3
    
    except Exception as e:
        #If something goes wrong, return none
        return None


def fetch_and_scrape(url, formatted_datetime):
    """
    Fetches URLs from GDELT and calls scrape_and_save_s3 function.

    Parameters:
    url (str): The URL to fetch data from.
    formatted_datetime (str): The formatted datetime string.

    Returns:
    None
    """
    try:
        #Get the current CSV column for the urls of that timestamp
        curr_url_list = pd.read_csv(
            url, 
            delimiter='\t', 
            header=None, 
            quotechar='"',
            escapechar='\\',
            on_bad_lines='skip'
        )[url_col_idx].unique().tolist()
        
        #Call the function to scrape the urls and save them to the S3 bucket
        return scrape_into_df(curr_url_list, formatted_datetime)
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV at {formatted_datetime}: {e}")
        #Add date to the skipped ones
        skipped_dates.append(formatted_datetime)
        #And return a None value 
        return None
    except Exception as e:
        logger.error(f"Error inside scrape_and_save_s3 function: {e}")
        #Add date to the skipped ones
        skipped_dates.append(formatted_datetime)
        #And return a None value 
        return None


def news_to_scrape_to_s3(start_date_str, end_date_str, concurrent_threads=5):
    """
    Collects news URLs from GDELT between two dates and saves the scraped content to an S3 bucket.

    Parameters:
    start_date_str (str): The start date in 'YYYY-MM-DD HH:MM:SS' format.
    end_date_str (str): The end date in 'YYYY-MM-DD HH:MM:SS' format.

    Returns:
    None

    Raises:
    ValueError: If the start date is after the end date or if the seconds/minutes are not aligned.
    Exception: If there is an error during the scraping or saving process.
    """
    # Define base URL format
    base_url = "http://data.gdeltproject.org/gdeltv2/{datetime}.export.CSV.zip"
    
    # Convert strings to datetime
    start_date = pd.to_datetime(start_date_str, format='%Y-%m-%d %H:%M:%S')
    end_date = pd.to_datetime(end_date_str, format='%Y-%m-%d %H:%M:%S')
    
    # Validate dates
    if start_date >= end_date:
        raise ValueError("Start date must be before end date.")
    
    if start_date.second != 0 or end_date.second != 0:
        raise ValueError("Seconds must be 0.")
    
    if start_date.minute % 15 != 0 or end_date.minute % 15 != 0:
        raise ValueError("Minutes must be 0, 15, 30, or 45.")
    
    #Generate URLs
    current_date = start_date
    total_iterations = (end_date - start_date) // timedelta(minutes=15) + 1
    
    urls_to_scrape = []
    
    batch_size = start_date = int(os.getenv('BATCH_SIZE_SILVER', 20))  # Number of dfs per batch
    accumulated_results = []
    
    for _ in range(total_iterations): 
        # Generate the url for the current iteration
        formatted_datetime = current_date.strftime('%Y%m%d%H%M%S')
        url = base_url.format(datetime=formatted_datetime)
        urls_to_scrape.append((url, current_date))
        current_date += timedelta(minutes=15)
    
    #Initialize Cleaner
    cleaner_saver = CleanerSaver(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        max_length=10000, 
        min_length=500
    )
    
    # Process URLs in batches
    for i in range(0, len(urls_to_scrape), batch_size):
        batch_urls = urls_to_scrape[i:i + batch_size]
        
        # Use ThreadPoolExecutor to process URLs in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_threads) as executor:
            futures = [executor.submit(fetch_and_scrape, url, date) for url, date in batch_urls]
            
            # Process the futures as they complete
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(batch_urls), desc="Processing URLs"):
                try:
                    result = future.result()
                    if result is not None and not result.empty:
                        accumulated_results.append(result)
                except Exception as e:
                    logger.error(f"Error in fetch_and_scrape function: {e}")
        
        # Join the data, clean it, and save to S3 in parquet format after each batch
        if accumulated_results:
            join_dfs_clean_and_save(accumulated_results=accumulated_results, cleaner_saver=cleaner_saver)
            accumulated_results = []  # Reset the accumulated results

    # Handle any remaining accumulated results
    if accumulated_results:
        
        #Join the data, clean it and save to S3 in parquet format
        join_dfs_clean_and_save(accumulated_results=accumulated_results, cleaner_saver=cleaner_saver)

def retry_skipped_dates():
    """
    Retries fetching and scraping URLs for the skipped dates.

    Returns:
    None
    """
    if not skipped_dates:
        logger.info("No skipped dates to retry.")
        return

    print(f"Retrying skipped dates...")

    #Convert datetime objects to strings for retry
    skipped_urls_to_scrape = [(f"http://data.gdeltproject.org/gdeltv2/{date.strftime('%Y%m%d%H%M%S')}.export.CSV.zip", date) for date in skipped_dates]

    for url, date in tqdm(skipped_urls_to_scrape, desc="Retrying skipped dates"):
        try:
            fetch_and_scrape(url, date)
        except Exception as e:
            logger.error(f"Failed to process URL for {date}: {e}")

if __name__ == "__main__":

    if not start_date or not end_date:
        logger.error("Error: START_DATE and END_DATE environment variables must be set.")
        exit(1)

    #Call executer function
    try:
        news_to_scrape_to_s3(start_date_str=start_date, end_date_str=end_date, concurrent_threads=concurrent_threads)

        #Print the final message and the dates that have been skipped
        logger.info(f"All news collected! Skipped dates: {skipped_dates}")

        #If inidcated, try and collect those skipped dates
        if retry_skipped_dates_arg == "yes":

            logger.info(f"Sleeping before retrying...")
            time.sleep(10)

            retry_skipped_dates()

        #Display finish message
        logger.info("Finished!")

    except Exception as e:
        logger.error(e)
        exit(0)