FROM python:3.9
RUN CHROMEDRIVER_VERSION=`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE` && \
mkdir -p /opt/chromedriver-$CHROMEDRIVER_VERSION && \
curl -sS -o /tmp/chromedriver_linux64.zip http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip && \
unzip -qq /tmp/chromedriver_linux64.zip -d /opt/chromedriver-$CHROMEDRIVER_VERSION && \
rm /tmp/chromedriver_linux64.zip && \
chmod +x /opt/chromedriver-$CHROMEDRIVER_VERSION/chromedriver && \
ln -fs /opt/chromedriver-$CHROMEDRIVER_VERSION/chromedriver /usr/local/bin/chromedriver

# Install Google Chrome
RUN curl -sS -o – https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add – && \
echo “deb http://dl.google.com/linux/chrome/deb/ stable main” >> /etc/apt/sources.list.d/google-chrome.list && \
apt-get -yqq update && \
apt-get -yqq install google-chrome-stable && \
rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y \
unzip \
curl \
gnupg \
&& rm -rf /var/lib/apt/lists/*


RUN curl -sS -o – https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add

RUN echo “deb http://dl.google.com/linux/chrome/deb/ stable main” >> /etc/apt/sources.list.d/google-chrome.list

RUN apt-get -y update

RUN apt-get install -y google-chrome-stable

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY scrap.py .

CMD [ “python”, “scrap.py” ]
