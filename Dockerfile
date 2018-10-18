FROM java:8

ARG version

COPY target/pg2k4j-$version-jar-with-dependencies.jar /pg2k4j-jar-with-dependencies.jar
COPY docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["--help"]