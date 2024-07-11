#Use an official Python runtime as a parent image
FROM python:3.9-slim

#Set the working directory
WORKDIR /usr/src/app

#Copy the current directory contents into the container at /usr/src/app
COPY . .

#Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

#Copy env file into the container
COPY .env .

#Run the executor script when the container launches. Specify the number of files to be processed at the same time
# by default, the dockerfile is though to be launched with AWS batch, so the execution mode is 'batch'
CMD ["python", "executor.py", "20", "batch"]