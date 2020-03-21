FROM python:3

WORKDIR /app
# instruction to be run during image build
RUN pip3 install pandas
RUN pip3 install pandasql
RUN pip3 install fastparquet
# Copy all the files from current source duirectory(from your system) to
# Docker container in /app directory
COPY . /app
CMD [ "python3", "./app.py" ]

