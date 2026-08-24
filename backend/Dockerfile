   FROM python:3.11-slim

   WORKDIR /app

   RUN apt-get update && apt-get install -y --no-install-recommends \
       ca-certificates gcc libpq-dev curl \
       && update-ca-certificates \
       && rm -rf /var/lib/apt/lists/*

   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt

   COPY . .
   RUN chmod +x /app/entrypoint.sh

   EXPOSE 8000

   CMD ["/app/entrypoint.sh"]