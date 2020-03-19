FROM python:latest
RUN pip3 install pandas
RUN pip3 install pandasql
RUN pip3 install fastparquet

COPY main.py /

CMD [ "python3", "./main.py" ]
