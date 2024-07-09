# data_cleaner

This package will take the news collected from the **gdelt_news_collector** module, clean them and store them in another bucket in '.parquet' format, leaving the data ready and prepared to use.

There are three componentes:
- **cleaner.py**: The object that will be used for cleaning the body of the scraped news.
- **loader.py**: In charge of taking the CSVs from the `collector_bucket` and remove the old CSVs once they have been transformed and saved into the `clean_bucket`.
- **executor.py**: Script containing all the logic to execute the ETL process. It takes a batch of CSVs, clean them, add the date as a column and save into the `clean_bucket` as a single '.parquet' file.
  - Execution command is: python executor.py <number_of_files_to_process> <execution_mode>
    - <number_of_files_to_process>: The 'batch size', indicates how many CSVs will be processed at each iteration. CSVs processed in the same batch will be stored in the same '.parquet' file.
    - <execution_mode>: `continuous` or `batch`.

### continuous <execution_mode>

It will start processing CSVs in blocks of the specified <number_of_files_to_process> (batch_size) and iterate in the cleaning process until the `collector_bucket` is empty.

### batch <execution_mode>

It will only take a single batch of CSVs, process them (clean), and then die. This <execution_mode> is though to be deployed in a cloud environment, in a event-programmed way each 15 minutes, so it works in synchronized way with the **gdelt_news_collector/real_time_collector**. In that way, you can ensure to have clean and ready-to-use data with a real-time granularity.
