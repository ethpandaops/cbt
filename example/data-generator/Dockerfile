FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the generator script
COPY generate.py .

# Run the generator
CMD ["python", "-u", "generate.py"]