FROM python:3.10.2-slim-buster

ENV TZ=Asia/Singapore

ENV PROJ_WORKDIR="/code"
WORKDIR $PROJ_WORKDIR

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD ["/usr/local/bin/python", "./src/tabulate_my_scrobble.py"]
