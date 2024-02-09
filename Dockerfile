# oracle_fdw extension with Postgres 16.1
#
# This script uses pgxn client:
#   https://pgxn.github.io/pgxnclient/
#
# Run the commands below and you will get a container with the oracle_fdw extension available for use:
#   docker build -t oracle_fdw .
#   docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 oracle_fdw
#   docker exec -it some-postgres psql -h localhost -U postgres
#       SELECT * FROM pg_available_extensions where name = 'oracle_fdw';
#       CREATE EXTENSION oracle_fdw;
#       SELECT oracle_diag();
#           Output:
#           oracle_fdw 2.6.0, PostgreSQL 16.1 (Debian 16.1-1.pgdg120+1), Oracle client 21.13.0.0.0
#

FROM postgres:16.1

# Install all dependencies to compile oracle_fdw
RUN apt-get update && \
    apt-get install -y postgresql-server-dev-16 build-essential libkrb5-dev \
    flex libaio1 libaio-dev wget unzip pgxnclient

# Download and Install Oracle Instant Client
# instantclient 21.13 x86_64 (64-bit)
RUN wget https://download.oracle.com/otn_software/linux/instantclient/2113000/instantclient-basic-linux.x64-21.13.0.0.0dbru.zip
RUN wget https://download.oracle.com/otn_software/linux/instantclient/2113000/instantclient-sdk-linux.x64-21.13.0.0.0dbru.zip
RUN unzip instantclient-basic-linux.x64-21.13.0.0.0dbru.zip
RUN unzip instantclient-sdk-linux.x64-21.13.0.0.0dbru.zip
RUN ln -s instantclient_21_13 instantclient
RUN echo /instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf
RUN ldconfig
RUN ln -s /instantclient/libclntsh.so /lib/libclntsh.so

# Setting the C_INCLUDE_PATH environment variable to find the Oracle Instant Client source
ENV C_INCLUDE_PATH=/instantclient/sdk/include

# Downloading, compiling and installing the latest version of the extension
RUN pgxn install oracle_fdw