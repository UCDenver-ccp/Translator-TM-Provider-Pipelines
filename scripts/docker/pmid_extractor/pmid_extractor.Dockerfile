#
# To run:
#      docker build -t pmidext -f pmid_extractor.Dockerfile .
#      docker run --rm -v [path_to_xml_directory]:/home/input -v [path_to_output_directory]:/home/output pmidext
#
FROM amazoncorretto:8

RUN yum -y install maven wget vim less git tar procps && yum -y clean all && rm -rf /var/cache

COPY pom.xml tm-pipelines-bundled-0.1.0.jar /home/code/

WORKDIR /home/code

CMD ["mvn", "exec:exec"] 