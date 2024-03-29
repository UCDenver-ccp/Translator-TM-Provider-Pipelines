from api import *
import os
import sys
import gzip
import logging
import datetime
import time


# probably needs to be put in /home/code/phenobert/phenobert/utils in order for the "from api" import to work

# parse the input file and annotate chunks where each chunk is an individual document
# modified from https://stackoverflow.com/questions/42964022/how-to-start-reading-from-a-file-at-a-certain-line/42964612#42964612
def document_iter(file):
    """Iterates through a large file, returning a chunk of text for each distinct
    document as demarcated by comment lines"""
    id = None
    block = ""
    for line in file:
        # find ID
        if "###C: DOCUMENT_ID" in line:
            block = ""
            id = line.split("\t")[1]
            # build a list of lines until the DOCUMENT_END comment is seen
            for line in file:
                line = line.strip()
                if line == "###C: DOCUMENT_END":
                    break
                if "###C:" not in line:
                    block = block + line + "\n"
            # return the compiled block
            yield id, block


def fix_phenobert_output(id, raw_phenobert_output):
    """Taks as input the output of phenobert and removes some incorrect line breaks. 
    It appears that when there is a line break following a matched concept, the line break 
    is included in the output file, e.g.,

    25037   25051   IOP depression  HP:0000490      0.99
    26156   26170   increased IOP
    HP:0007906      1.00
    26245   26257   hypertension    HP:0000822      1.00

    This function looks for output lines wiht only 3 columns and appends those lines to the 
    next line assuming the next line doesn't start with a number and has < 5 columns.
    """
    fixed_phenobert_output = ""
    prev_line = None
    for line in raw_phenobert_output.split("\n"):
        if line:
            # only process lines with content
            cols = line.split("\t")
            if (len(cols) >= 5):
                # then we assume this line to be correctly formatted
                fixed_phenobert_output = fixed_phenobert_output + line + "\n"
            else:
                if prev_line == None:
                    prev_line = line
                else:
                    # there is a previous line, so let's check to make sure the current 
                    # line doesn't start with a number
                    if not cols[0].isnumeric():
                        # then we will connect this line to the previous line, removing the incorrect line break
                        updated_line = f"{prev_line}\t{line}\n"
                        fixed_phenobert_output = fixed_phenobert_output + updated_line
                        prev_line = None
                    else:
                        # the first column is a number - not sure what to do here - hopefully this is not possible
                        # this shouldn't be possible, but we will set the prev_line to line in this case 
                        prev_line = line
                        logging.warning("Unexpected error in phenobert raw output in {id}. Short line starting with numbers ({line}) observed after short line {prev_line}.")
    return fixed_phenobert_output



# phenobert output lines look like the following:
# 0       27      Hypertrophic cardiomyopathy     HP:0001639      1.00
# 29      56      Hypertrophic cardiomyopathy     HP:0001639      1.00
# 79      101     cardiovascular disease  HP:0001626      1.00
# 5103    5128    normal growth of children       HP:0008897      1.00    Neg
def convert_to_bionlp_format(phenobert_output):
    """Takes as input phenotype annotations in the phenobert output format and
    returns the annotations in bionlp format. There are some annotations tagged 
    with 'Neg'. We will ignore these for now as their meaning is unclear. It may
    represent the opposite of an HP concept?"""
    bionlp = ""
    t_index = 1
    for line in phenobert_output.split("\n"):
        if line:
            # only process lines with content
            cols = line.split("\t")
            if len(cols) > 5 and cols[5] == 'Neg':
                # skip lines tagged with Neg
                continue
            span_start= cols[0]
            span_end = cols[1]
            covered_text = cols[2]
            concept_id = cols[3]
            bionlp_line = f"T{t_index}\t{concept_id} {span_start} {span_end}\t{covered_text}\n"
            t_index = t_index + 1
            bionlp = bionlp + bionlp_line

    return bionlp
    # return phenobert_output


def serialize_output(document_id, bionlp, file):
    """serializes the phenotype annotations for a given document; the id is stored
    in a comment line to divide data for each document."""
    file.write(f"###C: DOCUMENT_ID\t{document_id}\n")
    file.write(f"{bionlp}\n")
    file.write(f"###C: DOCUMENT_END\n")


# # for testing locally
# with open("/Users/bill/projects/ncats-translator/prototype/tm-pipelines.git/scripts/docker/phenobert/CRAFT.BATCH_1.phenobert.bionlp", mode="rt", encoding="utf-8") as in_file:
#     for id, datablock in document_iter(in_file):
#         print(f"{id}\n{datablock}")
#         fixed = fix_phenobert_output(id, datablock)
#         convert_to_bionlp_format(fixed)

# for testing locally
# with gzip.open(
#     "/Users/bill/projects/ncats-translator/prototype/tm-pipelines.git/scripts/docker/phenobert/all.txt.gz", mode="rt", encoding="utf-8"
# )as in_file:
#     for id, datablock in document_iter(in_file):
#         print(f"{datablock}")

# # testing monotonically increasing time
# ab = """Hypertrophic cardiomyopathy

# Hypertrophic cardiomyopathy is a common inherited cardiovascular disease present in one in 500 of the general population. It is caused by more than 1400 mutations in 11 or more genes encoding proteins of the cardiac sarcomere. Although hypertrophic cardiomyopathy is the most frequent cause of sudden death in young people (including trained athletes), and can lead to functional disability from heart failure and stroke, the majority of affected individuals probably remain undiagnosed and many do not experience greatly reduced life expectancy or substantial symptoms. Clinical diagnosis is based on otherwise unexplained left-ventricular hypertrophy identified by echocardiography or cardiovascular MRI. While presenting with a heterogeneous clinical profile and complex pathophysiology, effective treatment strategies are available, including implantable defibrillators to prevent sudden death, drugs and surgical myectomy (or, alternatively, alcohol septal ablation) for relief of outflow obstruction and symptoms of heart failure, and pharmacological strategies (and possibly radiofrequency ablation) to control atrial fibrillation and prevent embolic stroke. A subgroup of patients with genetic mutations but without left-ventricular hypertrophy has emerged, with unresolved natural history. Now, after more than 50 years, hypertrophic cardiomyopathy has been transformed from a rare and largely untreatable disorder to a common genetic disease with management strategies that permit realistic aspirations for restored quality of life and advanced longevity.
# """
# start = time.time()
# for i in range(1,100):
#     annotate_text(ab)
#     end = time.time()
#     print(f"{i} -- elapsed time: {end - start}s")
#     start = end


with gzip.open(
    "/home/code/input/all.txt.gz", mode="rt", encoding="utf-8"
) as in_file, gzip.open(
    "/home/code/output/phenobert.bionlp.gz", mode="wt", encoding="utf-8"
) as out_file:
    
    for id, datablock in document_iter(in_file):
        ct = datetime.datetime.now()
        start = time.time()
        # annotate the input file in single document chunks
        phenobert_output = annotate_text(datablock)
        # remove incorrect line breaks in phenobert_output
        fixed_phenobert_output = fix_phenobert_output(id, phenobert_output)
        # convert the annotations from the annotated chunk to bionlp format
        pheno_annots_bionlp = convert_to_bionlp_format(fixed_phenobert_output)
        # store the output in chunks in a single, compressed file
        serialize_output(id, pheno_annots_bionlp, out_file)
        end = time.time()
        logging.warning(f"processed {id} at {ct} -- datablock length: {len(datablock)} -- elapsed time: {end - start}")

# TODO: re-run text extraction pipeline (on CRAFT and PUBMED_SUB_37)
# TODO: test phenobert run on CRAFT
# TODO: write component to ingest phenobert bionlp files from bucket and store in cloud datastore as individual bionlp files
# TODO: add phenotype bionlp files to the concept post-processing input and make sure they get used/loaded by the post-processing code
# TODO: determine if the concept post-processing code should apply the disease CRF to the PhenoBERT annotations - I think probably not
# TODO: decide if we should exclude OGER HP annotations (assuming we have PhenoBERT working at scale)
