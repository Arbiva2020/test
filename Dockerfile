# Specify base image(+tag):
FROM python:3.9
# Add file into container:
ADD main.py .
# Install dependencies:
RUN pip install requests pandas multitasking numpy yfinance pymongo yahoofinancials
# The command to execute when the container is started:
CMD ["python", "./main.py"]

## docker -v
## build docker image: docker -t python-imdb
## docker run pythom-imdb


#### Alternative:
#FROM python:3.9-alpine3.15 (the slim version for saving space)
#WORKDIR /app
#COPY ./requirements.txt .
#RUN pip install -r requirements.txt
#COPY . .
# ENTRYPOINT ["python3", "app.py"]

