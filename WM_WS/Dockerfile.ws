FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka
COPY protocol.py ./
COPY WM_WS/ ./WM_WS/
ENTRYPOINT ["python", "-u"]
