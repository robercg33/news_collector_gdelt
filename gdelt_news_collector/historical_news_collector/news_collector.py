import pandas as pd
import os
import sys
from datetime import datetime, timedelta
import boto3
import json
from tqdm import tqdm
from dotenv import load_dotenv
import concurrent.futures
import time

#Load the environment
load_dotenv()

#Get the credentials from the environment
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

#The bucket to store the news
s3_bucket_name = os.getenv('S3_COLLECTOR_BUCKET_NAME')
#And the lambda function name
lambda_function_name = os.getenv('LAMBDA_SCRAPER_FUNCTION_NAME')


s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

lambda_client = boto3.client(
    'lambda',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

#Take count of the dates skipped, either by error or by max_retries in the lambda fucntion call
skipped_dates = []
url_col_idx = 60

def scrape_and_save_s3(url_list, date_of_file):
    """
    Scrapes the provided URLs and saves the results to the S3 bucket provided in the .env file.

    Parameters:
    url_list (list of str): A list of URLs to scrape.
    date_of_file (datetime): The date of the urls, provided by GDELT in the file collected.

    Returns:
    None
    """
    #If we reach the maximum lambda request, wait and try after sleeping, with exponential backoff (max 5 times)
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            #Call created lambda function with the list of urls "url_list"
            response = lambda_client.invoke(
                FunctionName=lambda_function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps({"urls": url_list})
            )
            break
        #If the exception is because we have reached the maximum concurrecy allowed by lambda, wait,  up to 5 retries
        except lambda_client.exceptions.TooManyRequestsException:
            print(f"Too many requests. Retrying in {2 ** retries} seconds...")
            time.sleep(2 ** retries)
            retries += 1
        #Otherwise, consider a failed operation and avoid retrying
        except Exception as e:
            print(f"Error inside scrape_and_save_s3 function for date {date_of_file}: {e}")
            #Add date to the skipped ones
            skipped_dates.append(date_of_file)
            return
    #If we have reached the maximum retries, skip this file an exit the function
    if retries == max_retries:
        print(f"Failed to invoke Lambda function after {max_retries} retries. Skipping date {date_of_file}.")
        #Add date to the skipped ones
        skipped_dates.append(date_of_file)
        return
    
    #Get the response payload
    response_payload = json.load(response['Payload'])

    #Convert the response payload from string to a list of dictionaries
    response_list = json.loads(response_payload)
    #Create the DF to store into S3
    df_for_s3 = pd.DataFrame(response_list)

    #Drop the rows with NaN values
    df_for_s3 = df_for_s3.dropna()

    #Save the response from the lambda function into a csv in S3
    result_filename = f"news_{date_of_file.strftime('%Y_%m_%d__%H_%M_%S')}.csv"

    df_for_s3.to_csv(result_filename, index=False, escapechar="\\")

    #Our time to write to S3
    s3_client.upload_file(result_filename, s3_bucket_name, result_filename)

    #Delete the local result file
    os.remove(result_filename)


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
        scrape_and_save_s3(curr_url_list, formatted_datetime)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV at {formatted_datetime}: {e}")
        #Add date to the skipped ones
        skipped_dates.append(formatted_datetime)
    except Exception as e:
        print(f"Error inside scrape_and_save_s3 function: {e}")
        #Add date to the skipped ones
        skipped_dates.append(formatted_datetime) 


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
    
    for _ in range(total_iterations): 
        # Generate the url for the current iteration
        formatted_datetime = current_date.strftime('%Y%m%d%H%M%S')
        url = base_url.format(datetime=formatted_datetime)
        urls_to_scrape.append((url, current_date))
        current_date += timedelta(minutes=15)
    
    #Use ThreadPoolExecutor to process URLs in parallel with progress bar
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_threads) as executor:
        futures = [executor.submit(fetch_and_scrape, url, date) for url, date in urls_to_scrape]
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_iterations, desc="Processing URLs"):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing URL: {e}")

def retry_skipped_dates():
    """
    Retries fetching and scraping URLs for the skipped dates.

    Returns:
    None
    """
    if not skipped_dates:
        print("No skipped dates to retry.")
        return

    print(f"Retrying skipped dates...")

    #Convert datetime objects to strings for retry
    skipped_urls_to_scrape = [(f"http://data.gdeltproject.org/gdeltv2/{date.strftime('%Y%m%d%H%M%S')}.export.CSV.zip", date) for date in skipped_dates]

    for url, date in tqdm(skipped_urls_to_scrape, desc="Retrying skipped dates"):
        try:
            fetch_and_scrape(url, date)
        except Exception as e:
            print(f"Failed to process URL for {date}: {e}")

if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: python script.py <start_date> <end_date> <concurrent_threads> <retry_skipped_dates>")
        exit(1)

    #Get start and end dates
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    #Get concurrent threads
    try:
        concurrent_threads = int(sys.argv[3])
    except ValueError:
        print("Error: <concurrent_threads> must be an integer.")
        exit(1)

    #Get the parameter to retry to get the skipped dates or not
    retry_skipped_dates_arg = sys.argv[4]
    if retry_skipped_dates.lower() not in ["yes", "no"]:
        print("Error: <retry_skipped_dates> must be either 'yes' or 'no'")
        exit(1)

    #Call executer function
    try:
        news_to_scrape_to_s3(start_date_str=start_date, end_date_str=end_date, concurrent_threads=concurrent_threads)

        #Print the final message and the dates that have been skipped
        print(f"All news collected! Skipped dates: {skipped_dates}")

        #If inidcated, try and collect those skipped dates
        if retry_skipped_dates_arg == "yes":

            print(f"Sleeping before retrying...")
            time.sleep(10)

            retry_skipped_dates()

            print("Finished!")


    except Exception as e:
        print(e)
        exit(0)

