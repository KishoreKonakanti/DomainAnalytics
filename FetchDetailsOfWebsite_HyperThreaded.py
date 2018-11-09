# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 16:11:15 2018

@author: kkonakan
"""


from urllib import request as ureq
import bs4
import re
from numpy import NaN
import os
import csv
import time
import threading

class FetchDetailsOfWebsite(threading.Thread):    
    '''
        This class has all the required methods to get multiple properties of a website
        Link content retrieval has been parallelized using threads
        Each thread retrieves the content of a website in parallel
        Number of links per thread will be determined by numLinks
    '''
    
    def __init__(self, DOMAIN, writer, linkCount, linkBase, csvOutputFile):
            '''
            Constructor which takes the following details:
                    inFile: File containing the list of links
                    DOMAIN: Domain name 
                    writer: CSV writer to write the content to an outFile
                    numLinks: Number of links this thread crawls
                    lock: Lock which will be used, as and when required
        '''
    
        super(FetchDetailsOfWebsite, self).__init__()
        self.inFile = 'D:/AI/%s.txt'%DOMAIN # Input links file
        self.DOMAIN = DOMAIN
        self.writer = writer # CSV output writer
        self.crawledList = set()
        self.log = 0 # No log
        self.lock = threading.Lock()
        self.baseName = None
        self.linkBase = linkBase
        self.linkCount = linkCount
        self.skipLinks = skipLinks
        self.csvOutputFile = csvOutputFile
        
    def getAlexaRank(self,url):
            '''
            Given an url, retrieves Alexa Rank from alexa.com
        '''
        if self.log == 1:
            print('getAlexaRank')
        base_url = 'https://www.alexa.com/siteinfo/'
        comp_url = base_url+url
        html,_ = self.download(comp_url)
        html = str(html)
        rank = None
        try:
            rank = re.findall('.*global\":([\d].*?)}.*',html)[0]
            return rank
        except Exception:
            return None
    
    
    def getHostCountry(self,url):
        '''
            Given an url, retreives the hosted server of the website from alexa
        '''
        if self.log == 1:
            print('getHostCountry')
        addr='http://data.alexa.com/data?cli=10&url=%s'%url
        _,S = self.download(addr)
        country = None
        for i in S.find_all('country'):
            country = re.findall(r'.*?name=\"(.*?)\".*', str(i))[0]
        return country
    
    
    def saveHTML(self,url, content):
        return True # Bypassing saveHTML
        '''
            Saves the file to the disk to do offline analysis
            As of now, this is turned off by uncommenting the first line
            If you need an offline copy, please comment the first line of the function
                with an #
        '''
        
        if self.log == 1:
            print('saveHTML')
        if content is None:
            print('THERE IS NOTHING TO WRITE, RECEIVED NONE')
            return -1
        
        global path
        fname = None
        size = 0
        
        try:
            self.lock.acquire()
            #baseName = self.getBaseName(url)
            fname = 'D:/AI/Dataset/%s/%s_%s.html'%(self.DOMAIN.upper(),\
                                                   self.baseName, self.DOMAIN)
            print(fname,' is name of the file')
            ufile = open(fname, 'w')
            ufile.writelines(str(content))
            ufile.close()
            self.lock.release()
            size= round(os.path.getsize(fname)/1024)
        except Exception as e:
            print('Error occured during %s saving: %s'%(url,e))
        if self.log == 1:
            print('Returning successfully from saveHTML')
        return size
    
    def download(self,url):
        '''
            Downloads the website from the url provided in the function parameter 'url'
            Passes this content to BeautifulSoup to ease analysis of the content
        '''
        
        if self.log == 1:
            self.pr('download requested for ', url)
        soup = None
        html = None
        self.lock.acquire()
        try:
            html = ureq.urlopen(url, timeout=30).read()
            soup = bs4.BeautifulSoup(html,'lxml')
        except Exception as e:
            print('DOWNLOAD:',url,' site is not allowing bots with stack as ',e)
        #self.pr('Returning html,soup')
        self.lock.release()
        return html,soup
    
    def fillNans(self,siteDetails):
        '''
            Not all websites provides the information required for our analysis. 
            You should be filling NaNs to standardize 
        '''
        if self.log == 1:
            print('fillNans')
        global props
        for prop in props:
            if(siteDetails.get(prop,-12345) == -12345):
                siteDetails[prop] = NaN
            else: pass
        return 0
    
    def flatten(self, multiLine):
        if multiLine is None:
            return None
        if(len(multiLine) == 1):
            return multiLine
        retL = ''
        for l in multiLine:
           retL = '%s %s'%(retL, l)
        return retL
    
    def populateSiteDetails(self,url):
        '''
            For each url, provide complete information in "siteDetails" using 
            above methods
        '''
        if self.log == 1:
            print('populate')
        '''
            Retrieves site Details
            Saves the html content to disk for later processing
        '''
        
        html = None
        contentSoup = None
        siteDetails= {}
        html,contentSoup = self.download(url)
        if contentSoup is not None and contentSoup.title is not None:
            #print('Content Soup is not NONE:', contentSoup.title)
            siteDetails['url'] = url.strip()
            siteDetails['numLinks'] = len(contentSoup.find_all('a'))
            siteDetails['title'] = self.flatten(contentSoup.title.string)
            siteDetails['hostedIn'] = self.getHostCountry(url)
            #siteDetails['RegIn'] = getCountryReg(url)
            siteDetails['AlexaRank'] = self.getAlexaRank(url)
            #siteDetails['CSS'] = self.isUsingCSS(html)
            siteDetails['JS'] = self.isUsingJS(contentSoup)
            siteDetails['size'] = self.saveHTML(url, html)
            for metaTags in contentSoup.find_all('meta'):
                #print('Populating Tags')
                attrs = metaTags.attrs
                if ('name' in attrs.keys() and 'content' in attrs.keys()): # Reading meta tags
                    name = attrs['name']
                    dets = attrs['content']
                    if (name == 'title'):
                        siteDetails['title'] = self.flatten(dets)
                    elif (name == 'description' ):
                        siteDetails['descr'] = self.flatten(dets)
                    elif(name == 'keywords'):
                        siteDetails['kwords'] = self.flatten(dets)
                    else:
                        pass
                else: pass
        else: 
            siteDetails['url'] = url
            siteDetails['title']= self.getBaseName(url).upper()
        self.crawledList.add(url)
        return siteDetails
            
    def getBaseName(self,url):
        '''
             Retrieve the basename of a link
             Eg: Basename of https:www.google.ml  is google
        '''
        #print('Incoming ',url)
        import re
        pattern = '[whtps:/.]{0,12}([\w\W\d\.]{1,})\.*\.%s'%self.DOMAIN.lower()
        baseName= re.findall(pattern, url)[0]
        #print('URL:%s->%s'%(url,baseName))
        return baseName
    
    def transform(self,link):
        '''
            Standardize the link
        '''
        if link is None or link == '' :
            return None
        if self.log == 1:
            print('Incoming link:',link)
        exact_pattern = 'http[s]{0,1}://[w.]{0,4}[\w\W\d]*?\.%s$'%self.DOMAIN.lower()
        extra_pattern = 'http[s]{0,1}://[w.]{0,4}([\w\W\d]*)?\.%s\/.*$'%self.DOMAIN.lower()
        base_url = None
    
        if(re.match(exact_pattern, link)):
            self.baseName = re.findall(extra_pattern,link)[0]
            base_url = link
        elif(re.match(extra_pattern, link)):
            self.baseName = re.findall(extra_pattern,link)[0]
            base_url = 'https://www.%s.%s'%(self.baseName,self.DOMAIN.lower())
        else:pass
    
        if base_url is None:
            self.pr('INVALID Link:%s'%link)
    #    else:
     #       self.pr('Returning %s'%base_url)
        return base_url
    
    def pr(self,message):
        print('%s => %s'%(self.name, message))
        return None
        
    
    def run(self):
        '''
            Entry call for the thread
        '''
        cnt = 0
        global start_time
        
        self.linkCount = len(linkBase)
       
        for link in self.linkBase:
            #print('HERE')
            
            self.pr('Current Site# %d / %d: %s'%(cnt,self.linkCount,link ))
            cnt += 1

            siteDets = self.populateSiteDetails(link)
            self.pr('Length of sitedetail values: %d'%len(siteDets.values()))
            
            self.lock.acquire()
            # WRITE SITEDETAILS AND INCREMENT THE NUMBER OF POPULATED SITES BY 1
            self.writer.writerow(siteDets)
            
            populateCount[self.name] += 1
            self.lock.release()
            
        global completedThreadCount
        completedThreadCount += 1
        self.pr('**********************Time for this thread to complete: %d seconds'%(time.time()-start_time))
    def getLineCount(fname):
        with open(fname,'r') as foo:
            return len(foo.readlines())
    


# GLOBALS
def getLineCount(fname):
        with open(fname,'r',encoding='utf-8-sig') as foo:
            return len(foo.readlines())

def getCrawledLinks(fname):
    print('Incoming %s'%fname)
    if os.path.isfile(fname):
        import pandas as pd
        print('%s is available'%fname)
        df = pd.read_csv(fname, encoding='ISO-8859-1')
        urls = df['url'].unique()
        crawledLinks = set(urls)
    else:
        return set([])
    print('Number OF already CRAWLED Links:',len(crawledLinks))
    return crawledLinks



props = ['url','title','descr','kwords','AlexaRank','hostedIn']
thread_set = []
DOMAIN_list = ['ML']

populateCount = {}
completedThreadCount = 0
crawledLinks = set()
start_time = time.time()
path = 'D:/AI/DataSet'

try:
    '''
        numLinksPerThread determines number of links each thread has to crawl
        sl_seconds will be the time between each thread spawn
    '''
    numLinksPerThread = 25
    sl_seconds = 30
    
    for DOMAIN in DOMAIN_list:
        
        print('DOMAIN:%s'%DOMAIN)
        
        csvOutputFile='D:/AI/Dataset/%s.csv'%DOMAIN # Output file
        linkFile = 'D:/AI/%s.txt'%DOMAIN 
        
        allLinks = []
        '''
         In case you are running this file for the second time, we will not be 
              crawling the sites which were already crawled
        '''
        crawledLinks = getCrawledLinks(csvOutputFile)
        
        if os.path.isfile(linkFile):
            with open(linkFile,'r', encoding='utf-8-sig') as LFile:
                print('Reading links')
                for link in LFile.readlines():
                    link = link.strip()
                    if link in crawledLinks: 
                        pass
                    else:
                        allLinks.append(link.strip())
                        
        print('To Crawl link count: %d'%len(allLinks))            
        csvfile=open(csvOutputFile, 'a', encoding='utf-8-sig', buffering=1)
        writer = csv.DictWriter(csvfile, props, restval=NaN)
        
        availableLinkCount = len(allLinks)
        
        if availableLinkCount%numLinksPerThread == 0:
            threadCount = availableLinkCount//numLinksPerThread
        else:
            threadCount = (availableLinkCount//numLinksPerThread) + 1
        
        print('NUMBER OF THREADS WILL BE %d'%threadCount)
        
        skipLinks = 0
        threadNum = 0
        st = time.time()
        
        writer.writeheader()
        
        while threadNum < threadCount:
            
            threadName = '%s: %d'%(DOMAIN, threadNum)
            skipLinks = numLinksPerThread*threadNum    
            linkBase = set(allLinks[skipLinks:skipLinks+numLinksPerThread])
            threadNum += 1
            current = FetchDetailsOfWebsite(DOMAIN, writer, numLinksPerThread, linkBase, csvOutputFile)

            current.setName(threadName)
            populateCount[threadName] = 0
            thread_set.append(current)
            print('\nStarting a thread for %s'%(DOMAIN))
            current.start()        
            skipLinks += numLinksPerThread
            time.sleep(sl_seconds)  
        
        if availableLinkCount%numLinksPerThread != 0:
            threadNum -= 1
        if threadCount > threadNum:
            linkBase = allLinks[skipLinks:]
            print('Thread %d: Remaining %d links'%len(linkBase))
            current = FetchDetailsOfWebsite(DOMAIN, writer, len(linkBase), linkBase)
            current.name = '%s-LAST'%DOMAIN
            thread_set.append(current)
            current.start()
    
    for thread in thread_set:
         name = thread.name
         print('Waiting for thread %s to join'%name)
         thread.join(timeout=7200)
         print('NUMBER OF LINKS COVERED by thread %s ARE',name,populateCount[name])
         if(thread.is_alive()):
           print('Despite timeout of 2 hours, thread %s is still alive'%name)
           
             
except threading.ThreadError as the:
    print('Thread error stack:',the)
    
except Exception as e:
    print('==>{0} for {1} thread has been raised'.format(e, current.name.upper()))
    
finally:
    if(current.isDaemon()):
        print('Daemon reporting =====> Time taken:%d seconds'%(time.time() - start_time))
        print('Daemon reporting =====> PopulateCount',populateCount)
        
print('--->Time taken:%d seconds'%(time.time() - start_time))
