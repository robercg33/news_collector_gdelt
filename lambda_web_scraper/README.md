# lambda_web_scraper

This package contains the necessary files to deploy a function into a cloud environment (AWS lambda or equivalent) which receives a list of urls of news articles and returns the title and body associated with each url.

## Components

- **lambda_scraper.py**: The script of the function
- **python-layer.zip**: Zip file containing the python environment that should be provided to the AWS lambda function in order to execute the script
- **test_lambda.txt**: An example of test in JSON format to check proper functioning of the function

You can also use only the **lambda_scraper.py** script and integrate in your local environment to keep everything locally.
