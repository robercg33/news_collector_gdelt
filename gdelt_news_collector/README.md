# news_collector_gdelt

This package contains two sub-packages that are used to collect news from the GDELT page. The two packages are:

## historical_news_collector

Used to collect news from anytime in between 2015 and the current date.

- Command for **news_collector.py**: python news_collector.py <start_date> <end_date> <concurrent_threads> <retry_skipped_dates>
  - <start_date>: Start date for collecting news. Format must be "YYYY-mm-dd HH:MM:SS"
  - <end_date>: End date for collecting news. Format must be "YYYY-mm-dd HH:MM:SS"
  - <concurrent_threads>: Number of concurrent threads to be using by the script
  - <retry_skipped_dates>: Must be either "yes" or "no". When some specific datetime fail to get the news, it will save it on a list. At the end of the execution, if this parameter was set to "yes", it will try to collect again all failed datetimes.

## real_time_collector

Used to collect news in real time, as GDELT updates with new articles (in english) each 15 minutes.

- Command for **last_csv_collector.py**: python last_csv_collector.py

## Containerized deployment

Specially, the **real_time_collector** package is though to be deployed on a contunuously running environment. Make sure the VM where you deploy them have the necessary environment variables (specified in the root directory of the project) either by setting them up on the VM or in the Dockerfile.
