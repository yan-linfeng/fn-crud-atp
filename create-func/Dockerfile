FROM oraclelinux:9-slim

WORKDIR /function

RUN groupadd --gid 1000 fn && adduser --uid 1000 --gid fn fn

RUN microdnf -y install python3 python3-pip && \
    microdnf clean all

RUN microdnf install oracle-instantclient-release-23ai-el9
RUN microdnf install oracle-instantclient-basic

RUN mkdir /tmp/dbwallet
RUN chown -R fn:fn /tmp/dbwallet

ENV LD_LIBRARY_PATH=/usr/lib/oracle/23/client64/lib
ENV PATH=/usr/lib/oracle/23/client64/bin:$PATH

ENV TNS_ADMIN=/tmp/dbwallet

ADD . /function/

RUN pip3 install --upgrade pip

RUN pip3 install --no-cache --no-cache-dir -r requirements.txt

RUN rm -fr /function/.pip_cache ~/.cache/pip requirements.txt func.yaml Dockerfile README.md

ENV PYTHONPATH=/python

ENTRYPOINT ["/usr/local/bin/fdk", "/function/func.py", "handler"]