FROM maven:3.5.2-jdk-8

RUN apt-get update && \
    apt-get install -y python-pip && \
    pip install awscli

COPY src /src
COPY pom.xml /pom.xml
COPY integrationtests_resources /integrationtests_resources
COPY newrelic/newrelic.yml /target/newrelic/newrelic.yml

RUN mvn clean install test

CMD [ "sh", "-c", "java $JAVA_OPTS -jar /target/kirby-pg2k4j-1.0-SNAPSHOT-jar-with-dependencies.jar " ]

ARG build_number
ARG build_timestamp
ARG build_url
ARG git_branch_name
ARG git_sha1
ARG git_url
ARG project_name
ARG project_version

LABEL net.bamgrid.build.number=${build_number} \
      net.bamgrid.build.timestamp=${build_timestamp} \
      net.bamgrid.build.url=${build_url} \
      net.bamgrid.discover.dockerfile="/Dockerfile" \
      net.bamgrid.discover.packages="apk show -v | sort" \
      net.bamgrid.git.branch-name=${git_branch_name} \
      net.bamgrid.git.url=${git_url} \
      net.bamgrid.git.sha-1=${git_sha1} \
      net.bamgrid.project.name=${project_name} \
      net.bamgrid.project.url=${git_url} \
      net.bamgrid.version=${project_version}
