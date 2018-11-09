# DomainAnalytics
Code to reproduce results as described in domain analytics @ <a>https://medium.com/@enigma2006.kishore/analysis-about-ai-ml-io-domains-6a5cdac91a46" </a>

Packages used:
==============

**Common**: re, time, threading, os, csv, pandas, numpy, collection, urllib

**May require a new install**:

 bs4(Beautiful Soup (<a> https://www.crummy.com/software/BeautifulSoup/bs4/doc/ </a>)
 
 wordcloud (<a> https://github.com/amueller/word_cloud</a>)
 
 nltk (<a> www.nltk.org </a>)
 
 langid (<a> https://github.com/saffsd/langid.py.git </a): An offline module to detect language
    
Description of script files:
============================
PullLinks.py: Loads links to a file (called as linkFile)

FetchDetailsOfWebsite_HyperThreaded.py: For each link read in step 1, load required details of a website and write to a csv file

WebsiteAnalysis.py: Use Pandas to read csv file and do analysis, draw graphs


To Do:
  1. Add a parameter file to read required parameters so as to avoid messing with the code



