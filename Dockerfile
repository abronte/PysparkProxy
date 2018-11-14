FROM adambronte/pyspark_proxy_base

RUN mkdir -p /srv/pyspark_proxy

WORKDIR /srv/pyspark_proxy

ADD . /srv/pyspark_proxy

RUN pip install -r requirements.txt

ENV PYTHONPATH /srv/pyspark_proxy
