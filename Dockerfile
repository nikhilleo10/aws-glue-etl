FROM amazon/aws-glue-libs:glue_libs_3.0.0_image_01
RUN pip3 install -U pip setuptools wheel
RUN pip3 install -U spacy
RUN pip3 install -U scispacy
RUN pip3 install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.1/en_ner_bc5cdr_md-0.5.1.tar.gz