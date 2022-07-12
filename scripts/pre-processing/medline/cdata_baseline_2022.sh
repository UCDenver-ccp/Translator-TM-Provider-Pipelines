#!/bin/bash

# process in batches of 100 to keep disk usage to a minimum

# testing
#gunzip -v /mnt/disks/pmc-oa-work/medline/temp/*.gz
#echo 1
#sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/temp/*.xml
#echo 2
#sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/temp/*.xml
#echo 3
#sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/temp/*.xml
#echo 4
#sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/temp/*.xml
#gzip -v /mnt/disks/pmc-oa-work/medline/temp/*.xml

#exit 0


gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n00*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

#exit 0


gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n01*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n02*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n03*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n04*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n05*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n06*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n07*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n08*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n09*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml

gunzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/pubmed22n1*.xml.gz
echo 1
sed -i 's/<ArticleTitl\([^>]*[^\/]\)>/<ArticleTitl\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 2
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 3
sed -i 's/<AbstractTex\([^>]*[^\/]\)>/<AbstractTex\1><![CDATA[/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
echo 4
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml
gzip -v /mnt/disks/pmc-oa-work/medline/baseline_2022/*.xml


