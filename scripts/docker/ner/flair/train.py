from flair.embeddings import TransformerWordEmbeddings
from flair.models import SequenceTagger
from flair.trainers import ModelTrainer
from flair.data import Corpus
from flair.datasets import ColumnCorpus

# define columns
columns = {0 : 'text', 1 : 'ner'}
# directory where the data resides
data_folder = '/home/flair/input_data'
# initializing the corpus
corpus: Corpus = ColumnCorpus(data_folder, columns,
                              train_file = 'train.tsv',
                              test_file = 'test.tsv',
                              dev_file = 'dev.tsv')

# 2. what label do we want to predict?
label_type = 'ner'

# 3. make the label dictionary from the corpus
label_dict = corpus.make_label_dictionary(label_type=label_type, add_unk=False)
print(label_dict)

# 4. initialize fine-tuneable transformer embeddings WITH document context
# embeddings = TransformerWordEmbeddings(model='xlm-roberta-large',
embeddings = TransformerWordEmbeddings(model='microsoft/BiomedNLP-PubMedBERT-base-uncased-abstract-fulltext',                                       
                                       layers="-1",
                                       subtoken_pooling="first",
                                       fine_tune=True,
                                       use_context=True,
                                       model_max_length=512
                                       )

# 5. initialize bare-bones sequence tagger (no CRF, no RNN, no reprojection)
tagger = SequenceTagger(hidden_size=256,
                        embeddings=embeddings,
                        tag_dictionary=label_dict,
                        tag_type='ner',
                        use_crf=False,
                        use_rnn=False,
                        reproject_embeddings=False,
                        )

# 6. initialize trainer
trainer = ModelTrainer(tagger, corpus)

# 7. run fine-tuning
trainer.fine_tune('/home/flair/ner_model',
                  learning_rate=5.0e-6,
                  mini_batch_size=4,
                  mini_batch_chunk_size=1,  # remove this parameter to speed up computation if you have a big GPU
                  )