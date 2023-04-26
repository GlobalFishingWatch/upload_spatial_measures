# What do you need to run this?
# I would like to have the OCR/GDAL for tiff images
# And also I want to have the python libraries to stablish the connections
FROM osgeo/gdal:alpine-normal-latest as gdal-commands
RUN mkdir /app/
WORKDIR /app/
RUN apk add --no-cache py3-pip


# Credentials
VOLUME ["/root/.config"]

# Install the dependencies
COPY ./requirements.txt ./
RUN ls -lh && pip install -r requirements.txt

# Install package
COPY . /app/
# RUN pip install -e .
RUN python -m pip install -e .

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["./main.py"]
