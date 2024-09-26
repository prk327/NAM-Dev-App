# Use the official Jupyter Notebook image with Python 3
FROM quay.io/jupyter/minimal-notebook:latest

COPY ./src/requirements.txt /tmp/

# Install the requirements
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Clean up
RUN rm /tmp/requirements.txt

# Set working directory inside the container
WORKDIR /home/jovyan/work

# Create a script to modify user based on environment variables
USER root
RUN apt-get update && apt-get install -y sudo

# Add a script to set the user permissions at runtime
COPY set_user.sh /usr/local/bin/set_user.sh
RUN chmod +x /usr/local/bin/set_user.sh

# Expose Jupyter port
EXPOSE 8888