# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Create a directory for persistent storage and set permissions
RUN mkdir -p /app/storage && chmod 777 /app/storage

# Install required Python packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 8080 to match Cloud Run's default port
EXPOSE 8080

# Define environment variables
ENV PORT=8080
ENV STORAGE_PATH=/app/storage

# Start the app using Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--workers=1", "--timeout=600", "--keep-alive=600", "app:app"]