FROM python:3.8
ENV PYTHONBUFFERED=1
WORKDIR /code
COPY . /code/
RUN pip install -r requirements.txt
ENTRYPOINT [ "python", "worker.py" ]