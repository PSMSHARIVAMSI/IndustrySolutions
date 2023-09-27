#!/usr/bin/env python
# coding: utf-8

# In[45]:


# import our libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup
import boto3
import pdfkit
from datetime import datetime


# In[48]:


def ScrapeSec(cik,form_type,dateb):
    

# base URL for the SEC EDGAR browser

            endpoint = r"https://www.sec.gov/cgi-bin/browse-edgar"
            headers =  {'User-Agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36'}



            # define our parameters dictionary
            param_dict = {'action':'getcompany',
                          'CIK':cik,
                          'type':form_type,
                          'dateb':dateb,
                          'owner':'exclude',
                          'start':'',
                          'output':'',
                          'count':'500'}

            # request the url, and then parse the response.
            response = requests.get(url = endpoint, params = param_dict,headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            company_name = soup.find('span',attrs = {'class' : 'companyName'}).text.split(',')[0].strip()

            # Let the user know it was successful.
            
            print(response.url)
            
            doc_table = soup.find_all('table', class_='tableFile2')



# define a base url that will be used for link building.
            base_url_sec = r"https://www.sec.gov"

            master_list = []
            for row in doc_table[0].find_all('tr')[0:300]:
                      #find all the columns
                cols = row.find_all('td')
                    # if there are no columns move on to the next row.
                if len(cols) != 0:  
                         # grab the text
                        filing_type = cols[0].text.strip()                 
                        filing_date = cols[3].text.strip()
                        filing_numb = cols[4].text.strip()

                        # find the links
                        filing_doc_href = cols[1].find('a', {'href':True, 'id':'documentsbutton'})       
                        filing_int_href = cols[1].find('a', {'href':True, 'id':'interactiveDataBtn'})
                        filing_num_href = cols[4].find('a')

                        # grab the the first href
                        if filing_doc_href != None:
                            filing_doc_link = base_url_sec + filing_doc_href['href'] 
                        else:
                            filing_doc_link = 'no link'

                        # grab the second href
                        if filing_int_href != None:
                            filing_int_link = base_url_sec + filing_int_href['href'] 
                        else:
                            filing_int_link = 'no link'

                        # grab the third href
                        if filing_num_href != None:
                            filing_num_link = base_url_sec + filing_num_href['href'] 
                        else:
                            filing_num_link = 'no link'

                        # create and store data in the dictionary
                        file_dict = {}
                        file_dict['file_type'] = filing_type
                        file_dict['file_number'] = filing_numb
                        file_dict['file_date'] = filing_date
                        file_dict['links'] = {}
                        file_dict['links']['documents'] = filing_doc_link
                        file_dict['links']['interactive_data'] = filing_int_link
                        file_dict['links']['filing_number'] = filing_num_link

                        # let the user know it's working
                        #print('-'*100)        
                        #print("Filing Type: " + filing_type)
                        #print("Filing Date: " + filing_date)
                        #print("Filing Number: " + filing_numb)
                        #print("Document Link: " + filing_doc_link)
                        #print("Filing Number Link: " + filing_num_link)
                        #print("Interactive Data Link: " + filing_int_link)

                        # append dictionary to master list
                        master_list.append(file_dict)
                

            master_list1=[]
            length = len(master_list)
            List10K=[]
            for i in range(0,length):
                List10K.append(master_list[i]['links']['documents'])
       
    
            for n in range(len(List10K)):
    
# request the url, and then parse the response.
                response = requests.get(url = List10K[n],headers=headers)
                soup1 = BeautifulSoup(response.content, 'html.parser')

# find the document table with our data
                doc_table = soup1.find_all('table', class_='tableFile')
    
                for row in doc_table[0].find_all('tr')[0:3]:
                    cols = row.find_all('td')
                    if len(cols) != 0:  
                        filing_type = cols[1].text.strip()  
                        filing_doc_href = cols[2].find('a', {'href':True})  


            # grab the the first href
                        if filing_doc_href != None:
                            filing_doc_link = base_url_sec + filing_doc_href['href'] 

                        else:
                            filing_doc_link = 'no link'


                        file_dict = {}
                        file_dict['links'] = {}
                        file_dict['links']['documents'] = filing_doc_link
                        file_dict['file_type'] = filing_type
                        file_dict['date_of_filing'] = soup1.find_all('div',{"class": "info"})[0].text


                        if(filing_type)==form_type:

                            #print("Filing Type: " + filing_type)
                            #print("Document Link: " + filing_doc_link)

                            #print('-'*100)

                            master_list1.append(file_dict)
#print(master_list1)   
            s3 = boto3.client("s3", 
                aws_access_key_id='AKIA3BVQT6DU5TTZFO6W',
                aws_secret_access_key='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')
            s3 = boto3.resource(
            service_name='s3',
            region_name='us-east-2',
            aws_access_key_id='AKIA3BVQT6DU5TTZFO6W',
            aws_secret_access_key='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')




            bucket_name = "secfilespoc"
            current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            str_current_datetime = str(current_datetime)
            bucket = s3.Bucket(bucket_name)

            path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
            config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
            counter=0
            for statement in master_list1:
                pdffromurl = pdfkit.from_url(statement['links']['documents'],configuration=config)
                response=bucket.put_object( ACL="private",Body=pdffromurl,Bucket=bucket_name,Key=company_name+'/'+form_type+'/'+statement['date_of_filing']+'.pdf')
            print(response)
        
               
            
            


# In[49]:


ScrapeSec('1265107','10-K','2023')


# In[ ]:





# In[ ]:





# In[ ]:




