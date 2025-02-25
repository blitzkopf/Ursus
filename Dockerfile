FROM python:3.13-slim


WORKDIR /opt/oracle

RUN apt-get update && apt-get install -y wget unzip libaio1 git && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip 

RUN wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip && \
    unzip instantclient-basiclite-linuxx64.zip && rm -f instantclient-basiclite-linuxx64.zip && \
    cd /opt/oracle/instantclient* && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci && \
    echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig

WORKDIR /opt/ursus

COPY dist/ursus-0.1.0-py3-none-any.whl .

RUN --mount=type=cache,target=/root/.cache pip install ursus-0.1.0-py3-none-any.whl
RUN mkdir /repos && git config --global --add safe.directory '*' && \
    git config --global user.email "ursus@miracle.is" && \
    git config --global user.name "Ursus"



ENTRYPOINT ["ursusd" , "-c", "ursus.conf"] 
