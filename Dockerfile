FROM apache/airflow:2.9.2

USER airflow
# Sao chép requirements.txt và cài đặt các gói Python
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

