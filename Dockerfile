## according to order:
# the python version we work with:
FROM python:3.9
# add file into the container - create source:
ADD main.py .
# install third-party libraries:
RUN pip install requests pandas multitasking numpy yfinance pymongo
# the command to execute when the container is started - "python" and then first parameter:
CMD ["pyton", "./main.py "]
