# -*- coding: utf-8 -*-
"""
Created on Sat Oct 27 12:56:39 2018

@author: kkonakan
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from langdetect import DetectorFactory
import re
import time
from collections import Counter

DetectorFactory.seed = 0
nonEng = []
EngWords = []

def genwordcloud(wordset,name):
    '''
        Builds a string of words in wordset and draws WordCloud 
    '''
    print('Incoming:',name)
    if wordset is None:
        return None
    kwstr = ''
    for word in wordset:
        kwstr = kwstr + ' ' + word

    wordcloud = WordCloud(width=1000, height=1000, background_color='white', 
                          stopwords=stopwords.words('english'),\
                          min_font_size=10).generate(kwstr)
    title= 'WordCloud for domain:'+name
    plt.figure(figsize=(14, 14))
    plt.imshow(wordcloud)
    plt.title(title.upper())
    plt.show()

def arankAnalysis(df,name):
    '''
        Steps
            1. Load AlexaRanks
            2. Convert to int using pd.to_numeric to 
                plot the data in plotAlexaRankPlot method
            3. Sort values
    '''
    global sortedars 
    AR = pd.DataFrame(df.AlexaRank)
    AR.dropna(inplace=True)
    castedAR = pd.DataFrame(pd.to_numeric(AR['AlexaRank'], errors='coerce', downcast='signed'))
    castedAR.dropna(inplace=True)
    sortedAR = sorted(list(castedAR.AlexaRank))
    sortedars[name]= sortedAR
        
def hostedAnalysis(df):
    '''
        Reads hostedIn data of the dataframe and groups by Country
    '''
    hin = df.groupby(['hostedIn']).size()
    hin_dict = pd.DataFrame(data=[],columns=['Country', 'Count'])
    loc = 0
    for ind in hin.index:
        country = ind
        count = hin[ind]
        hin_dict.loc[loc] = [country,count]
        loc += 1
    del hin
    
    return hin_dict


def getlangset(wordset):
    '''
        Considers only english words using langid
    '''
    import langid as lid
    engwords = set()
    for word in wordset:
        if len(word) > 2:
            lang, conf = lid.classify(word)
            if conf > 0.8:
                engwords.add(word)
        else:
            continue
    return engwords
  
def kwordAnalysis(df, name):
    '''
        Tokenizes key word list
        Calls genwordcloud to draw WordCloud          
    '''
    kwords = df.kwords
    kwords.dropna(inplace=True)
    wordset = set()
    sw = stopwords.words('english')
    for line in kwords:
        tokens = word_tokenize(line)
        [wordset.add(word) if len(word)>2 else None for word in tokens]
               
    wordset.difference_update(sw)
    Engwords = getlangset(wordset)
    
    genwordcloud(Engwords, name)
    
    return wordset
    del wordset
    
def hostedplot(gh):
    '''
        Draws a scatter plot of the hosted country
        Increases the size of a bubble by number of hosted servers
    '''
    country_list = list(gh.Country)
    total = list(gh.Total)
    y_coords = np.arange(len(gh))
    import matplotlib.pyplot as plt

    plt.figure(figsize=(15,15))
    plt.xscale('symlog')
    plt.ylabel('Countries')
    plt.xlabel('Number of websites hosted')
    sz = list(gh.Total)
    sz = [x*30 for x in sz]
    plt.grid(True)
    plt.scatter(gh.Total,y_coords, label='Total', s=sz)
    for i,country in enumerate(country_list):
        x_coord = total[i]
        y_coord = y_coords[i]
        if country == 'United States':
            country = 'USA'
        elif country == 'United Kingdom':
            country = 'UK'
        text = '%s (%d)'%(country, x_coord)
        fsize = 10 * (total[i]/100)
        if fsize < 10:
            fsize = 10
        #print('Fontsizes:', fsize)
        plt.text(x_coord,y_coord, text, fontsize=fsize)
    plt.title('Number of Hosted Websites per Country')
    plt.show()
    del plt

def plotAlexaRankPlot():
    '''
        A simple consolidated scatter plot comparing all the domains
        sortedars are populated in arankAnalysis method
    '''
    global sortedars 
    import matplotlib.pyplot as plt
    colors = ['r','b','g','k','m']

    plt.figure(figsize=(10,10))
    for (k,v) in sortedars.items():
        plt.scatter(v,np.arange(len(v)), label=k, c=colors.pop())
    
    plt.legend(loc='upper left')
    plt.grid()
    plt.title('Alexa Rank Analysis')
    plt.show()
    del plt


start_time = time.time()
sortedars = {}
    
#aranks = []
colors = ['r','b','g','k','m']
domains = ['AI','IO','ML']
hins = []

for domain in domains:
    data = pd.read_csv('D:/AI/Dataset/%s.csv'%domain)
    #print('DATA:%s'%file)
    arankAnalysis(data,domain)
    hin_df = hostedAnalysis(data)
    hins.append(hin_df)
    wset = kwordAnalysis(data,domain)

gh = pd.DataFrame(hins[0])
for i in range(1, len(hins)):
    gh = gh.merge(hins[i],on='Country', how='outer')

    gh = gh[ (gh.Country != 'hostedIn') ]

    gh.fillna(value=0,inplace=True)
    gh['Total'] = gh.Count_x + gh.Count_y + gh.Count

hostedplot(gh)

plotAlexaRankPlot()
print('Time taken:%d seconds'%(time.time() - start_time))