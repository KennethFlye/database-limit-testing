FROM python:3.9

#ENV Database (Måske mere ved Kubernetes)
ENV DATA_POINTS=1000 POOLS=10

RUN mkdir ./requirements

COPY requirements.txt ./requirements

RUN pip install -r ./requirements/requirements.txt

#RUN mkdir -p /home/scripts

COPY . .

CMD ["python", "./src/run_write_test.py"]