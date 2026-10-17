FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY db.py .
COPY listener.py .
COPY forwarder.py .

CMD ["python", "bot.py"]
