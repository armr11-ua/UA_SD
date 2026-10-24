FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka
# Monitor: solo protocolos.py. Engine: protocolos.py (Monitor) y topics.py (Kafka)
COPY protocolos.py topics.py ./
COPY WM_WS/ ./WM_WS/
ENV WS_STATE_DIR=/state
RUN mkdir -p /state
ENTRYPOINT ["python", "-u"]
