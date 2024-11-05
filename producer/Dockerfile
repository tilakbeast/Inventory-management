# Use the official Python image as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any dependencies required by the producer microservice
RUN pip install --no-cache-dir -r requirements.txt

# Expose any ports the microservice may be listening on
EXPOSE 8001

# Command to run the producer microservice
CMD ["python", "producer-1.py"]
