FROM python:3.7.12

RUN apt-get update && apt-get install -y \
    git \
    libsndfile-dev \
    less

RUN groupadd -r turku && useradd -m -r -g turku turku

RUN chown -R turku:turku /home/turku

USER turku
WORKDIR /home/turku

## Install the parser
WORKDIR /home/turku
RUN git clone https://github.com/TurkuNLP/Turku-neural-parser-pipeline.git ./Turku-neural-parser-pipeline.git

WORKDIR /home/turku/Turku-neural-parser-pipeline.git

ENV VIRTUAL_ENV=/home/turku/venv-tnpp
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

ARG MODEL=craft_dia
ARG PIPELINE=parse_plaintext
ARG PORT=7689
ARG MAX_CHARS=0

ENV TNPP_MODEL models_${MODEL}/pipelines.yaml
ENV TNPP_PIPELINE ${PIPELINE}
ENV TNPP_PORT ${PORT}
ENV TNPP_MAX_CHARS ${MAX_CHARS}

RUN python3 -m pip install --no-cache-dir requests
RUN python3 fetch_models.py $MODEL

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade setuptools

RUN python3 -m pip install --no-cache-dir torch==1.10.0
COPY requirements.fixed.txt /home/turku/
RUN python3 -m pip install --no-cache-dir -r /home/turku/requirements.fixed.txt

# fix compatibility bug
# See https://stackoverflow.com/questions/70193443/colab-notebook-cannot-import-name-container-abcs-from-torch-six
RUN perl -0777 -i.original -pe 's/if TORCH_MAJOR == 0:\n    import collections.abc as container_abcs\nelse:\n    from torch._six import container_abcs/if TORCH_MAJOR == 1 and TORCH_MINOR < 8:\n    from torch._six import container_abcs,int_classes\nelse:\n    import collections.abc as container_abcs\n    int_classes = int\n/igs' /home/turku/venv-tnpp/lib/python3.7/site-packages/apex/amp/_amp_state.py

EXPOSE ${TNPP_PORT}
#ENV FLASK_APP tnpp_serve
#CMD flask run --host 0.0.0.0 --port ${TNPP_PORT}
CMD waitress-serve --host 0.0.0.0 --port=${TNPP_PORT} 'tnpp_serve:app'