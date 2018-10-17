FROM java:8

ARG version

COPY target/pg2k4j-$version-jar-with-dependencies.jar /pg2k4j-jar-with-dependencies.jar
ENTRYPOINT ["java", "-jar", "/pg2k4j-jar-with-dependencies.jar"]
CMD ["--help"]