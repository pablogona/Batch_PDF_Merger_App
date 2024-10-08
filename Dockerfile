# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install required Python packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 8080 to match Cloud Run's default port
EXPOSE 8080

# Define the environment variable
ENV PORT=8080

# Start the app using Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--timeout", "120", "app:app"]
