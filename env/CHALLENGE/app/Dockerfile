# Use the official Python image as the base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file to the container's working directory
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application files to the container's working directory
COPY . .

# Expose the port that the Flask application will run on (replace 5000 with the appropriate port if necessary)
EXPOSE 5000

# Set the command to run the Flask application when the container starts
CMD ["python", "app.py"]