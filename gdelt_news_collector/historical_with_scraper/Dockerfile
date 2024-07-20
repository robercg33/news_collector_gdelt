# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Install required packages
RUN apt-get update && apt-get install -y dnsutils curl iputils-ping net-tools

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Copy the .env file into the container
COPY .env /app/.env

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run historical_collector.py when the container launches
CMD ["python", "historical_collector.py"]