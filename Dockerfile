FROM python:3.11.5-slim
RUN apt-get update && apt-get install -y libpq-dev gcc
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
COPY . ./
CMD ["python","app.py"]