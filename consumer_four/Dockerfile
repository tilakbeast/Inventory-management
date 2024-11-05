# Use the official Python image as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any dependencies required by the order processing consumer microservice
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the order processing consumer microservice
CMD ["python", "order_processing.py"]
