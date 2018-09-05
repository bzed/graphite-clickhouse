FROM devtools/go-toolset-7-rhel7

WORKDIR /go/src/github.com/lomik/graphite-clickhouse
COPY . .

RUN yum-config-manager --enable rhel-server-rhscl-7-rpms && \
    yum-config-manager --enable rhel-7-server-optional-rpms && \
    yum install -y golang make ca-certificates

RUN make && cp graphite-clickhouse /usr/local/bin/graphite-clickhouse
RUN mkdir -p /etc/graphite-clickhouse
RUN ./graphite-clickhouse -config-print-default | sed 's,/var/log/graphite-clickhouse/graphite-clickhouse.log,stdout,g' > /etc/graphite-clickhouse/config

CMD ["/usr/local/bin/graphite-clickhouse", "-config", "/etc/graphite-clickhouse/config"]

EXPOSE 2003/udp 2003/tcp 2004/tcp 2005/tcp 2006/tcp
