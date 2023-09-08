#!/bin/bash

./rrun-e-crf-batch.sh
wait
./rrun-f-oger-batch.sh
wait
./rrun-g-concept-post-processing-batch.sh