# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

#Copy env file into the container
COPY .env .

# The command to run the script will be provided by AWS Batch, so no CMD or ENTRYPOINT needed here.
CMD ["python", "historical_collector.py"]