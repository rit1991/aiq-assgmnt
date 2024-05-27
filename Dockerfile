# Create the base image
FROM python:3.8-slim

# Change the working directory
WORKDIR /app/

# Declare Arguments
ARG src="AIQ - Data Engineer Assignment - Sales data.csv"
ARG target="/app/"

# Install Dependency
COPY requirements.txt /app/
RUN pip install -r ./requirements.txt

# Copy local folder into the container
COPY aiq_pipeline.py /app/
COPY joblistconfig.py /app/
COPY constants.ini /app/
COPY AIQ_Sales_data.csv /app/


# Set "python" as the entry point
ENTRYPOINT ["python"]

# Set the command as the script name
CMD ["aiq_pipeline.py"]

#Expose the post 5000.
EXPOSE 5000