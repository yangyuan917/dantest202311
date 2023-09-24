FROM python:3.7.11-slim
WORKDIR /app_holding
COPY . /app_holding
RUN python -m pip install --upgrade pip
RUN pip3 install -r requirements.txt
CMD python app_holding.py