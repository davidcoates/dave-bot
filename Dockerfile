FROM python:3.10

WORKDIR /dave-bot

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dave-bot.py .
COPY features ./features
COPY lib ./lib

CMD ["python", "dave-bot.py"]
