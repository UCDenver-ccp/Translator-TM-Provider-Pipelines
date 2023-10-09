FROM gcr.io/deeplearning-platform-release/pytorch-gpu.1-13.py37

RUN apt-get update && apt-get install -y \
  less \
  && rm -rf /var/lib/apt/lists/*

RUN rm /bin/sh && ln -s /bin/bash /bin/sh

WORKDIR /root

# Installs google cloud sdk, this allows use of gsutil
# from: https://cloud.google.com/ai-platform/training/docs/custom-containers-training
RUN wget -nv \
    https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz && \
    mkdir /root/tools && \
    tar xvzf google-cloud-sdk.tar.gz -C /root/tools && \
    rm google-cloud-sdk.tar.gz && \
    /root/tools/google-cloud-sdk/install.sh --usage-reporting=false \
        --path-update=false --bash-completion=false \
        --disable-installation-options && \
    rm -rf /root/.config/* && \
    ln -s /root/.config /config && \
    # Remove the backup directory that gcloud creates
    rm -rf /root/tools/google-cloud-sdk/.install/.backup

# Path configuration
ENV PATH $PATH:/root/tools/google-cloud-sdk/bin

WORKDIR /home/code

RUN git clone https://github.com/TianLab-Bioinfo/PhenoBERT ./phenobert

WORKDIR /home/code/phenobert

RUN pip install -r requirements.txt && \
    python setup.py

WORKDIR /home/code/phenobert/phenobert/models

RUN curl -L -o bert_model_max_triple.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1AwRnaB5RruFUEdMkKohZmTlD4ILCkQ_z"

WORKDIR /home/code/phenobert/phenobert/models/HPOModel_H

RUN curl -L -o model_layer1.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=13RU9Kj5uNApdRn02i8tm0eFf7NuxfNY2"
RUN curl -L -o model_l1_0.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1NHXD4seiTkJAiCy8aoNViHc8kWzio1xh"
RUN curl -L -o model_l1_1.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=18PMqig_VbVWLyEy1VAhMbJL-7oHdB1vZ"
RUN curl -L -o model_l1_2.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=183u7m4bNYGUbbUwwC8R5d5NtkMKDwsZ2"
RUN curl -L -o model_l1_3.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1oQiuKfcLAZC4pJNUwZ8T4tGh8bakCx4x"
RUN curl -L -o model_l1_4.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1EPfOIl2zXntRIBwEzDhA4D7z5qkihpeW"
RUN curl -L -o model_l1_5.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1BYFucnMYX0bnThp9c6x2Fv28yA8GxjGK"
RUN curl -L -o model_l1_6.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1EIpMIrmD4JnH1xqd4HNc6uDlmiJuElGm"
RUN curl -L -o model_l1_7.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=12zf_uJUosWNBAobjo1tXVm-RboJZmMli"
RUN curl -L -o model_l1_8.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1mT8HU9w8tVkmo5hbOUREZH35pc8FSgab"
RUN curl -L -o model_l1_9.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1SiDn7nMQQK2L2FHjieN2cMtwQ5jghMnW"
RUN curl -L -o model_l1_10.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1CZbQ3b2XqA-Plz5J2ZP6Plpx7HB1XVu7"
RUN curl -L -o model_l1_11.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1YA-60nGx0ccESRvOniTozzDS6Bx7TLIF"
RUN curl -L -o model_l1_12.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1Fp0vLbYPiO1cLy2xKzy5vaNw9BhOB0MS"
RUN curl -L -o model_l1_13.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1ZoyPyAWkvzcPdsvZ3ksAs-BZ3jLcOosE"
RUN curl -L -o model_l1_14.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1sXmfNQFzy5cj1VwVt4WkDCFIsDQKpbuo"
RUN curl -L -o model_l1_15.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1qtJRwM1usoqscaDaM9WIKJRxAM6GRxVV"
RUN curl -L -o model_l1_16.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1FTpjfOUQsbQV6vf65S_W9gG5lVUUbeEi"
RUN curl -L -o model_l1_17.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1Fnr4sStjx-bafXRVZgLKhCpjhjxrkqQv"
RUN curl -L -o model_l1_18.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1JhjGJeJhaN8nCrgqBuT9vLrML-Oox1za"
RUN curl -L -o model_l1_19.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1nrUKWd98DuIFiSwtyk7ZTPcnPFsmIrYH"
RUN curl -L -o model_l1_20.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1_7EcUgUTmzxJYxgMF66mL7hn-CxvMYlG"
RUN curl -L -o model_l1_21.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=10yZCKGA4d4aRwEYvXw80Bj8dGIufmRfQ"
RUN curl -L -o model_l1_22.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1lGh8ngtAZhj-fIE4XxB-qyRwe9sQJVNc"
RUN curl -L -o model_l1_23.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1IFYATlWSZZy4oQ8aLpVUBsFk681MSS5-"
RUN curl -L -o model_l1_24.pkl "https://drive.google.com/uc?export=download&confirm=yes&id=1AFXzBW1SiqVkinoTlbx9xvnmoUO85Ae9"

WORKDIR /home/code/phenobert/phenobert/embeddings/biobert_v1.1_pubmed

RUN curl -L -o vocab.txt "https://drive.google.com/uc?export=download&confirm=yes&id=1ub-cyhxwZvIOWMPbn6Nl_2F6ESfcGJYW"
RUN curl -L -o pytorch_model.bin "https://drive.google.com/uc?export=download&confirm=yes&id=1mnxLng9nLlpTXQ7lv-NADMlJ67gFa0hv"
RUN curl -L -o config.json "https://drive.google.com/uc?export=download&confirm=yes&id=1JLNz8pIJw5-k0RG0KLf9I2ldNzKxm0MQ"

WORKDIR /home/code/phenobert/phenobert/embeddings

RUN curl -L -o fasttext_pubmed.bin "https://drive.google.com/uc?export=download&confirm=yes&id=1GFB3I46B50sDUHcSpu84jZKqJnIjc--B"

COPY entrypoint.sh /home/code/phenobert/phenobert/utils
COPY process-input-file.py /home/code/phenobert/phenobert/utils

RUN chmod 755 /home/code/phenobert/phenobert/utils/entrypoint.sh && \
    mkdir /home/code/input && \
    mkdir /home/code/output

WORKDIR /home/code/phenobert/phenobert/utils

ENTRYPOINT /home/code/phenobert/phenobert/utils/entrypoint.sh "$@" 

# To build:
# see cloudbuild.yml

# To run:
# docker run --rm [IMAGE_NAME]:[IMAGE_VERSION] [TXT_BUCKET] [COLLECTION] [BATCH] [OUTPUT_BUCKET]
#
# where: 
#  [IMAGE_NAME] = the name of the Docker image - which will be the same as the TASK_NAME
#  [IMAGE_VERSION] = the version of the Docker image - which will be the same as the TUNED_MODEL_VERSION
#  [TXT_BUCKET] = the full GCP path to where the text files to-be-processed are located, e.g. gs://xyz/txt/PUBMED_SUB_31/
#  [COLLECTION] = the name of the collection being processed, e.g. PUBMED_SUB_31, 2021_06_08
#  [BATCH] = a unique identifier for the batch (subset of the collection) that is being processed
#  [OUTPUT_BUCKET] = the output bucket where resulting phenotype annotations in BIONLP format will be placed, e.g. gs://xyz