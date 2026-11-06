# Koristimo python image
FROM python:3.12-slim

# Setujemo radni direktorijum app unutar containera
WORKDIR /app

# Kopiramo sve fajlove iz lokalnog foldera u /app unutar containera
COPY . .

# Instaliramo sve pakete iz requirements.txt fajla
RUN pip install --no-cache-dir -r requirements.txt

# Definišemo port na kojem će aplikacija slušati
EXPOSE 80

# Definišemo environment variable
ENV PERSON="Covece e"

# Definišemo komandu koja će se izvršiti kada se container pokrene
CMD ["python", "app.py"]