FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY wb_min_bot.py .

CMD ["python", "wb_min_bot.py"]

