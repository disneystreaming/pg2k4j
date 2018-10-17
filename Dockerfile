FROM java:8

ARG version

COPY target/pg2k4j-$version-jar-with-dependencies.jar /pg2k4j-jar-with-dependencies.jar
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /pg2k4j-jar-with-dependencies.jar"]