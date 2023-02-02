#
# To run:
#      docker build -t elastic2brat -f elasticsearch_to_brat_cli.Dockerfile .
#      docker run --rm -v [path_to_repo]:/home/input elastic2brat [command + args]
#
FROM amazoncorretto:8

RUN yum -y install maven wget vim less git tar procps && yum -y clean all && rm -rf /var/cache

COPY entrypoint.sh tm-pipelines-bundled-0.1.0.jar /home/code/

RUN mkdir /home/input

WORKDIR /home/code

RUN chmod 755 entrypoint.sh

ENTRYPOINT [ "/home/code/entrypoint.sh" ]