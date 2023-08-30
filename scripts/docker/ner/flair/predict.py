from flair.models import SequenceTagger
from flair.data import Sentence
from flair.data import Token
import os
import gzip
import time

# import flair, torch
# flair.device = torch.device("cuda:0")

# this slowed things down by 2 min, but allowed mini-batch-size to be 256
# os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "max_split_size_mb:512"


def process_sentences(
    sentences, out_file_index, output_dir, output_filename_prefix, model
):
    # print(f"processing {len(sentences)} sentences")

    predict_time_start = time.perf_counter()
    # predict tags and print
    model.predict(sentences)
    predict_time_end = time.perf_counter()

    # print(f"Processing complete. Writing results to file {out_file_index}")
    # with gzip.open(
    #     os.path.join(
    #         output_dir, f"{output_filename_prefix}.{out_file_index}.conll03.gz"
    #     ),
    #     mode="wt",
    #     encoding="utf-8",
    # ) as f:
    for sentence in sentences:
        # print(sentence.to_tagged_string())

        # create a mapping from token num to label
        token_to_label = {}
        for l in sentence.get_labels():
            # the unlabeled_identifier is a list of the form: ['Span[4:5]:', '"striatal"']
            unlabeled_identifier = l.data_point.unlabeled_identifier
            # print(f"unlabeled_identifier: {unlabeled_identifier} == type: {type(unlabeled_identifier)}")
            span_str = unlabeled_identifier.split(" ")[0]
            # print(f"span_str: {span_str} == type: {type(span_str)}")
            span_str = span_str[len("Span[") :]
            # print(f"span_str1: {span_str}")
            span_str = span_str[0 : len(span_str) - 2]
            # print(f"span_str2: {span_str}")
            start_token = int(span_str.split(":")[0])
            end_token = int(span_str.split(":")[1])
            for index, t in enumerate(range(start_token, end_token)):
                prefix = "I-"
                if index == 0:
                    prefix = "B-"
                label = f"{prefix}{l.value}\t{l.score}"
                token_to_label[t] = label
                # print(f"storing label: {label} for token {t}")
            # print(f"{l}\t{l.score}\t{l.value}\t{type(l)}\t{l.data_point.text}\t{type(l.data_point.unlabeled_identifier)}\n")

        # print('---------------')

        for index, token in enumerate(sentence.tokens):
            tag = "O"
            if index in token_to_label:
                tag = token_to_label[index]
        #     f.write(f"{token.text}\t{tag}\n")
        # f.write("\n")
    sentence_process_end = time.perf_counter()
    print(
        f"Processing complete. Index: {out_file_index} Sentences: {len(sentences)} predict_time: {predict_time_end - predict_time_start:0.4f}s; process_time: {sentence_process_end - predict_time_end:0.4f}s"
    )


def main():
    print("=======PROCESSING========")
    input_dir = "/home/flair/input_conll03"
    output_dir = "/home/flair/output_conll03"
    output_filename_prefix = "transformer_ner"
    model = SequenceTagger.load("ner_model_0.3/final-model.pt")
    sentences = []
    out_file_index = 0
    last = time.perf_counter()
    for filename in os.listdir(input_dir):
        print(
            f"collecting sentences from {filename}... sentence count: {len(sentences)}"
        )
        # read the CoNLL03 formatted file(s) and create Sentence objects
        with gzip.open(
            os.path.join(input_dir, filename), mode="rt", encoding="utf-8"
        ) as f:  # open in readonly mode
            tokens = []
            for line in f.readlines():
                if not line.strip():
                    # empty line is a sentence break
                    if len(tokens) > 0:
                        sentence = Sentence(text=tokens, use_tokenizer=False)
                        sentences.append(sentence)
                    tokens = []  # reset the tokens list
                elif line.startswith("#"):
                    # this is a document separator comment - we will treat it as its own sentence and as a single token
                    sentence = Sentence(text=[Token(line)], use_tokenizer=False)
                else:
                    # otherwise, we just create a new token
                    tokens.append(Token(line))

                if len(sentences) > 31:
                    now = time.perf_counter()
                    print(f"Processing interval: {now - last:0.4f}s")
                    last = now
                    process_sentences(
                        sentences,
                        out_file_index,
                        output_dir,
                        output_filename_prefix,
                        model,
                    )
                    out_file_index += 1
                    sentences = []

                # after the final line, create the final sentence
            if len(tokens) > 0:
                sentence = Sentence(text=tokens, use_tokenizer=False)
                sentences.append(sentence)

    # process any remaining sentences
    # print(f"Processing {len(sentences)} sentences...")
    process_sentences(
        sentences, out_file_index, output_dir, output_filename_prefix, model
    )


# create example sentence
# tokens1 = [Token("We"), Token("analyzed"), Token("variation"), Token("in"), Token("striatal"), Token("volume"), Token("and"), Token("neuron"), Token("number"), Token("in"), Token("mice"), Token("and"), Token("initiated"), Token("a"), Token("complex"), Token("trait"), Token("analysis"), Token("to"), Token("discover"), Token("polymorphic"), Token("genes"), Token("that"), Token("modulate"), Token("the"), Token("structure"), Token("of"), Token("the"), Token("basal"), Token("ganglia"), Token(".")]
# sentence1 = Sentence(text=tokens1, use_tokenizer=False, start_position=100)
# tokens2 = [Token("PPARδ"), Token("dysregulation"), Token("of"), Token("CCL20"), Token("/"), Token("CCR6"), Token("axis"), Token("promotes"), Token("gastric"), Token("adenocarcinoma"), Token("carcinogenesis"), Token("by"), Token("remodeling"), Token("gastric"), Token("tumor"), Token("microenvironment")]
# sentence2 = Sentence(text=tokens2, use_tokenizer=False, start_position=300)
# sentences = [sentence1, sentence2]

if __name__ == "__main__":
    main()
