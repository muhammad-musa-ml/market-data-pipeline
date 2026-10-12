FROM apache/hadoop:3.4.3

USER root
COPY conf/core-site.xml conf/hdfs-site.xml /opt/hadoop/etc/hadoop/
COPY bin/entry.sh /opt/mdp/entry.sh
RUN mkdir -p /data/nn /data/dn \
    && chown -R hadoop:users /data /opt/mdp \
    && chmod +x /opt/mdp/entry.sh
USER hadoop

ENTRYPOINT ["/usr/local/bin/dumb-init", "--", "/opt/mdp/entry.sh"]
CMD ["namenode"]
