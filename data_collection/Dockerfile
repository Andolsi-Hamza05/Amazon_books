# Dockerfile — 11 lines. Nothing more. Works forever.
FROM python:3.13-slim

# Install Chrome + matching ChromeDriver in ONE command
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget unzip ca-certificates gnupg \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && CHROME_VERSION=$(google-chrome --version | cut -d " " -f3 | cut -d. -f1-4) \
    && wget -O /tmp/cd.zip https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$CHROME_VERSION/linux64/chromedriver-linux64.zip \
    && unzip /tmp/cd.zip -d /tmp \
    && mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/* /var/lib/apt/lists/*

# Your /webdriver folder
RUN mkdir -p /webdriver && ln -sf /usr/local/bin/chromedriver /webdriver/chromedriver

# Poetry + your code
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir poetry==1.8.3 \
    && poetry config virtualenvs.in-project true \
    && poetry install --only main --no-interaction

COPY . .
RUN mkdir -p /app/bronze

CMD poetry run python data_collection.py && tail -f /dev/null