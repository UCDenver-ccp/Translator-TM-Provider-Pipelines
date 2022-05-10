FROM amazoncorretto:8

RUN yum -y install maven wget vim less git tar procps && yum -y clean all && rm -rf /var/cache

COPY pom.xml /home/code/
COPY src/main/resources /home/code/src/main/resources/

WORKDIR /home/code

# this downloads most of the maven dependencies -- allowing code changes to be 
# incorporated into the Docker image more quickly during development.
RUN mvn verify

COPY src/ /home/code/src/

RUN mvn clean package

COPY brat_to_bert.entrypoint.sh /home/

RUN chmod 755 /home/brat_to_bert.entrypoint.sh

ENV MAVEN_OPTS "-Xmx2G"

ENTRYPOINT ["/home/brat_to_bert.entrypoint.sh"]
