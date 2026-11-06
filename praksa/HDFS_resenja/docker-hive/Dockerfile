FROM bde2020/hive:2.3.2-postgresql-metastore

# python je potreban za TRANSFORM ... USING 'python ...' (Movie ratings primer)
# jessie je arhiviran, pa apt mora na archive.debian.org
RUN echo "deb http://archive.debian.org/debian jessie main" > /etc/apt/sources.list \
 && apt-get -o Acquire::Check-Valid-Until=false update \
 && apt-get install -y --force-yes python \
 && rm -rf /var/lib/apt/lists/*
