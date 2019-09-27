FROM ubuntu:16.04
#FROM tiangolo/uwsgi-nginx-flask:python3.6
LABEL maintainer="Azure App Service Container Images <appsvc-images@microsoft.com>"
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential

#need for pyodbc on ubuntu
# apt-get and system utilities
#uncomment next three lines in using FROM ubuntu:16.04 and comment Line 14 thru 16
RUN apt-get update && apt-get install -y \
    curl apt-utils apt-transport-https debconf-utils gcc build-essential g++-5\
    && rm -rf /var/lib/apt/lists/*

# RUN apt-get update && apt-get install -y \
#     curl apt-utils apt-transport-https debconf-utils gcc build-essential

# adding custom MS repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install SQL Server drivers
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql unixodbc-dev

# install SQL Server tools
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN /bin/bash -c "source ~/.bashrc"

# python libraries
RUN apt-get update && apt-get install -y \
    python-pip python-dev python-setuptools \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# install necessary locales
RUN apt-get update && apt-get install -y locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen
RUN pip install --upgrade pip

# install SQL Server Python SQL Server connector module - pyodbc
RUN pip install pyodbc

# install additional utilities
RUN apt-get update && apt-get install gettext nano vim -y
#end of pyodbc for ubuntu

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
EXPOSE 5000
CMD ["runserver.py"]
