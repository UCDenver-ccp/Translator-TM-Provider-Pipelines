# This file contains utility functions for dealing with Medline XML
import os
import gzip
import re

# wraps the article title and abstract text with CDATA fields - this is 
# necessary so that the XML parser doesn't fail when it encounters formatting
# elements, such as <b>, <i>, <sup>, etc., in the titles and abstracts.
#
# The python functions replace a previous approach based on sed. The original 
# sed commands are logged below for historical purposes only. The sed commands
# were introducing a CTRL-A in the replaced text for some reason - unsure why.
# #### sed -i 's/<ArticleTitle\([^>]*\)>/<ArticleTitle\1><![CDATA[/g' *.xml && 
# #### sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' *.xml && 
# #### sed -i 's/<AbstractText\([^>]*\)>/<AbstractText\1><![CDATA[/g' *.xml && 
# #### sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' *.xml",
def wrap_title_and_abstract_with_cdata(**kwargs):
    """Process each file in the specified directory by creating an updated 
    version whereby the abstract text and article title text has been wrapped
    in a CDATA field. This is necessary b/c there are HTML fields, e.g. <b>, 
    <i> in these fields and they cause the XML parser to break."""
    dir_path = kwargs['dir']
    for root, dirs, files in os.walk(os.path.abspath(dir_path)):
        for f in files:
            fpath = os.path.join(root, f)
            print(f'Wrapping CDATA for file: {fpath}')
            tmpf = f'{fpath}.cdata'
            with gzip.open(tmpf, mode="wt", encoding="utf-8") as out_file:
                with gzip.open(fpath, mode="rt", encoding="utf-8") as file:
                    for line in file:
                        updated_line = wrap_text_with_cdata(line)
                        out_file.write(updated_line)
            # copy/rename the temp file back to original file name
            os.rename(tmpf, fpath)
    return 'zip_to_load'


def wrap_text_with_cdata(line):
    """Use regular expressions to wrap the article title and abstract text 
    fields with CDATA - this is necessary b/c there are HTML fields, e.g. <b>,
    <i> in these fields and they cause the XML parser to break."""
    updated_line = line
    is_empty_article_title = re.match(r"\s*<ArticleTitle([^>]*)/>", updated_line)
    if not is_empty_article_title:
        p = re.compile(r"<ArticleTitle([^>]*)>")
        updated_line = p.sub(r'<ArticleTitle\1><![CDATA[', updated_line)
    p = re.compile(r"</ArticleTitle>")
    updated_line = p.sub(r']]></ArticleTitle>', updated_line)
    # note that there are instances of blank/empty abstract text fields, 
    # e.g. <AbstractText Label="REVIEWERS' CONCLUSIONS" NlmCategory="CONCLUSIONS"/>. 
    # For these, we do not want to add the CDATA tag so we check for these empty fields 
    # and skip the CDATA wrapping if found.
    is_empty_abstract_text = re.match(r"\s*<AbstractText([^>]*)/>", updated_line)
    if not is_empty_abstract_text:
        p = re.compile(r"<AbstractText([^>]*)>")
        updated_line = p.sub(r'<AbstractText\1><![CDATA[', updated_line)
    p = re.compile(r"</AbstractText>")
    updated_line = p.sub(r']]></AbstractText>', updated_line)
    return updated_line

