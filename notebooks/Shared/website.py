# Databricks notebook source
# Databricks notebook source
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 10:07:14 2019

@author: 809917
"""

#In[1]: importing Required packages
import configparser
import camelot
import urllib.request as req
from urllib.request import urlopen, URLError, HTTPError
from dateutil import parser as date_parser
import json
import logging
from bs4 import BeautifulSoup
import shutil
import os
import pandas as pd
import datetime
import glob
import fitz
import pyodbc
import datetime
import cv2
import numpy as np
import pytesseract
from wand.image import Image
import re

#Loging environment setup
logger = logging.getLogger('momentive')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("momentive_web.log", 'w')
fh.setLevel(logging.DEBUG)
ch = logging.FileHandler("momentive_web_error.log", 'w')
ch.setLevel(logging.ERROR)
formatter =logging.Formatter(fmt = '%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

#Declaring global variables for web source:
web_prod_list = []
metadata_list =[]
html_list_pd=[]
file_exist_audit_check =[]
# In[2]: Getting the input and output file path from config file
config = configparser.ConfigParser()
config.read("/dbfs/mnt/momentive-configuration/config-file.ini")
momen_url = config.get('mount_path', 'web_url').split(',')
products =  config.get('mount_path', 'web_products').split(',')
web_out_path = '/dbfs/mnt/momentive-sources-pih/website-pih/product-application-pih/raw/'
metadata_outpath = '/dbfs/mnt/momentive-sources-pih/website-pih/product-application-pih/metdata/'
silicone_elast_path = '/dbfs/mnt/momentive-sources-pih/website-pih/product-application-pih/silicone-elastomer-brochure/'

#checking the product metadata with audit control table data based on timestamp or fil_length to process the product file again or not:
def web_incremental_check(file,web_inc_flag,content_info,web_df):
  try:
    if not web_df.empty:        
        web_file_filt_df = web_df[web_df['file_name'] == file]
        if not web_file_filt_df.empty:
          file_exist_audit_check.append(file)
          for web_index in web_file_filt_df.index: 
            if content_info.get('Last-Modified') != None:
              if web_file_filt_df['updated'][web_index] != 'Not Available':  
                if date_parser.parse(web_file_filt_df['updated'][web_index])  < date_parser.parse(content_info.get('Last-Modified')):
                  logger.info('{} existing file but it has been updated in the website, So processing it again'.format(file))
                  web_inc_flag = 's'
              else:
                if int(web_file_filt_df['file_size'][web_index]) < int(content_info.get('Content-Length')):
                  logger.info('{} existing file but it has been updated in the website, So processing it again'.format(file))
                  web_inc_flag = 's'
            else:
                if int(web_file_filt_df['file_size'][web_index]) < int(content_info.get('Content-Length')):
                  logger.info('{} existing file but it has been updated in the website, So processing it again'.format(file))
                  web_inc_flag = 's'
        else:
          logger.info('{} new file from the website'.format(file))
          web_inc_flag = 's'
    else:
        logger.info('{} new file from the website'.format(file))
        web_inc_flag = 's'
    if web_inc_flag == '':
      logger.info('{} existing file but no changes done on the file, So we are using existing data'. format(file))
    return web_inc_flag
  except Exception as e:
    logger.error('Something went wrong in the web_incremental_check function', exc_info=True)


#checking only the unique files are processed filtering done based on the existing processed files 
def web_duplicat_file_check(file,write_flag,page_url,content_info):
  try:
    global metadata_list
    if page_url not in web_prod_list and len(metadata_list) !=0:
        meta_df = pd.DataFrame(metadata_list)
        meta_df_parse = meta_df[meta_df[0]==file]
        if not meta_df_parse.empty:
            for meta_index in meta_df_parse.index:
                if meta_df_parse[2][meta_index] != 'Not Available' and content_info.get('Last-Modified') != None:
                    if date_parser.parse(meta_df_parse[2][meta_index]) < date_parser.parse(content_info.get('Last-Modified')):  
                        meta_df_parse = meta_df[~(meta_df[0]==file)]
                        metadata_list = meta_df_parse.values.tolist()
                        write_flag = 's'
                else:
                    if meta_df_parse[1][meta_index] < content_info.get('Content-Length'):
                        meta_df_parse = meta_df[~(meta_df[0]==file)]
                        metadata_list = meta_df_parse.values.tolist()
                        write_flag = 's'    
        else:
            write_flag = 's'
    elif page_url in web_prod_list:
        logger.info('{} this file has been processed already'.format(file))
        write_flag = ''
    else:
        write_flag = 's'      
    return write_flag
  except Exception as e:
    logger.error('Something went wrong in the web_duplicat_file_check function', exc_info=True)

#writing the web files to the staging area for validation
def web_file_write(content_info,file,web_raw_path,content,mode,file_website_path):
  try:
    global metadata_list
    #writing pdf file into raw 
    with open(web_raw_path + file, mode) as f:
        f.write(content)
        f.close()
        logger.info('{} has been successfully copied from momentive website to the raw blob storage {}'        
        .format(file,web_out_path)) 
        
    #writing pdf file into staging -> pdf -> raw
#     if file.endswith('.pdf'):
#       with open(pdf_file_path + file, mode) as f:
#           f.write(content)
#           f.close()
#           logger.info('{} has been successfully copied from momentive website to the staging -> pdf -> raw blob storage {}'        
#           .format(file,pdf_file_path)) 
          
#     #writing pdf file into staging -> docx
#     elif file.endswith('.doc') or file.endswith('.docx'):
#        with open(docx_file_path + file, mode) as f:
#           f.write(content)
#           f.close()
#           logger.info('{} has been successfully copied from momentive website to the staging -> docx blob storage {}'        
#           .format(file,docx_file_path))
          
#     #writing pdf file into staging -> docx
#     elif file.endswith('.txt'):
#        with open(text_file_path + file, mode) as f:
#           f.write(content)
#           f.close()
#           logger.info('{} has been successfully copied from momentive website to the staging -> text blob storage {}'        
#           .format(file,text_file_path))
    
          
    file_meta_info =[]    
    if content_info.get('Content-Length') != None:
        file_length = content_info.get('Content-Length')

    if content_info.get('Last-Modified') != None:
        file_date = content_info.get('Last-Modified')
    else:
      file_date = 'Not Available'   
    file_meta_info.append(file)
    file_meta_info.append(file_length) 
    file_meta_info.append(file_date)
    file_meta_info.append(file_website_path)
    metadata_list.append(file_meta_info)
    return metadata_list
  except Exception as e:
    logger.error('Something went wrong in the web_file_write function', exc_info=True)

#Extracting the usage of product based on the categories identified using TaxonName from the momentive home page metadata json
def Product_category(index,web_out_path,web_df):
    try:
        write_flag = ''
        web_inc_flag = ''
        if index['PageUrl'].strip().lower().startswith('/en-us/'):
            response = urlopen('https://www.momentive.com'+ index['PageUrl'])
        else:
            response = urlopen(index['PageUrl'])
        content_info = response.info()
        #index['Last-Modified'] = content_info.get('Last-Modified')
        #index['Content-Length'] = content_info.get('Content-Length')
        logger.info('{} file-info : Last-Modified is {} and file-size is {} we will update this details on unstructure audit table'\
                      .format(index['PageTitle'].strip().replace('/', '-'), content_info.get('Last-Modified'), content_info.get('Content-Length')))
        
        file = index['PageTitle'].strip().replace('/', '-') + '.txt'
        web_inc_flag = web_incremental_check(file[:-4],web_inc_flag,content_info,web_df)
        page_url = 'https://www.momentive.com'+ index['PageUrl']
        if web_inc_flag == 's':
          write_flag = web_duplicat_file_check(file,write_flag,page_url,content_info)
        if write_flag == 's':
          web_prod_list.append(page_url)
          content = response.read()
          soup = BeautifulSoup(content, 'lxml')
          divTag = soup.find_all("div", {"class": "text-white-sm pad-sm-4-4 pad-md-3-3 contain-sm"})
          content = ''
          for tag in divTag:
              tdTags = tag.find_all("p")
              for tag in tdTags:
                  content = content + tag.text + '\n'
          file_write = web_file_write(content_info,file,web_out_path,content,'w')
    except Exception as e:
      logger.error('Something went wrong in the Product_category function', exc_info=True) 
          
#Extracting the files for each product based on the metadata json from momentive home page for web source:        
def Product_Extract(index,web_out_path,web_df):
    try:
        if '?' in index['PageUrl'].split('/')[-1]:
          write_flag = ''
          web_inc_flag = ''
          response = urlopen(index['PageUrl'])
          content_info = response.info()
          #index['Last-Modified'] = content_info.get('Last-Modified')
          #index['Content-Length'] = content_info.get('Content-Length')
          logger.info('{} file-info : Last-Modified is {} and file-size is {} we will update this details on unstructure audit table'\
                      .format(index['PageTitle'].strip().replace('/', '-'), content_info.get('Last-Modified'), content_info.get('Content-Length')))
          if content_info.get('Content-Type') != None:
            file_extension = content_info.get('Content-Type').split('/')[-1]
          else:
            file_extension = 'pdf'
          file = index['PageTitle'].strip().replace('/', '-') + '.' + file_extension
          web_inc_flag = web_incremental_check(file[:-4],web_inc_flag,content_info,web_df)
          page_url = index['PageUrl']
          if web_inc_flag == 's':
            write_flag = web_duplicat_file_check(file,write_flag,page_url,content_info)
          if write_flag == 's':
              web_prod_list.append(page_url)
              content = response.read()
              file_write = web_file_write(content_info,file,web_out_path,content,'wb',index['PageUrl'])
        else:
            #Product_category(index,web_out_path,web_df)
            response = urlopen(index['PageUrl'])
            content_info = response.info()
            content_type = content_info.get('Content-Type').split('/')[-1]
            logger.info('{} its a {} so it cannot be processed'.format(index['PageUrl'],content_type))
            html_list=[]
            html_list.append(index['PageUrl'])
            html_list.append(content_type)
            html_list_pd.append(html_list)
                    
    except Exception as e:
      logger.error('Something went wrong in the Product_Extract function', exc_info=True)
    finally:
        df1 = pd.DataFrame(metadata_list,columns=['File_name', 'File_size','Date','website_path'])


#products bind in the related documents list from the momentive home page 
def Related_doc(index,web_out_path,web_df):
    try: 
        if index["RelatedDocuments"] is not None and index["RelatedDocuments"] != []:
            for relate_doc in index["RelatedDocuments"]:   
              if '?' in relate_doc['DocumentUrl'].split('/')[-1]:
                  write_flag = ''
                  web_inc_flag = ''
                  response = urlopen(relate_doc['DocumentUrl'])
                  content_info = response.info()
                  #relate_doc['Last-Modified'] = content_info.get('Last-Modified')
                  #relate_doc['Content-Length'] = content_info.get('Content-Length')
                  logger.info('{} file-info : Last-Modified is {} and file-size is {} we will update this details on unstructure audit table'\
                   .format(relate_doc['DocumentTitle'].strip().replace('/', '-') , content_info.get('Last-Modified'), content_info.get('Content-Length')))
                  if content_info.get('Content-Type') != None:
                    file_extension = content_info.get('Content-Type').split('/')[-1]
                  else:
                    file_extension = 'pdf'
                  file = relate_doc['DocumentTitle'].strip().replace('/', '-') + '.' + file_extension
                  web_inc_flag = web_incremental_check(file[:-4],web_inc_flag,content_info,web_df)
                  page_url = relate_doc['DocumentUrl']
                  if web_inc_flag == 's':
                    write_flag = web_duplicat_file_check(file,write_flag,page_url,content_info,)
                  if write_flag == 's':
                      web_prod_list.append(page_url)
                      content = response.read()
                      file_write = web_file_write(content_info,file,web_out_path,content,'wb',relate_doc['DocumentUrl'])
    except Exception as e:
      logger.error('Something went wrong in the Related_doc function ', exc_info=True)
    
# Creating the output dir for website Source
def path_exists(file_path):
  try:
    if file_path is not None and file_path != '':
      dbutils.fs.rm(file_path.replace("/dbfs",""),True)
      dbutils.fs.mkdirs(file_path.replace("/dbfs","dbfs:"))
      logger.info('Successfully created website blob stoarge {} '.format(file_path))
    else:
      logger.error('Website output is None or empty ')    
  except Exception as e:
      logger.error('Something went wrong in the path_exists function ', exc_info=True) 
      
# Fetching the website data from sql for incremental load
def Control_table_website_check(sql_conn):
  try:
    #delete
    cursor = sql_conn.cursor()
    #delete_query = "delete from momentive.unstructured_control_table where source_type = 'Website'"
    #cursor.execute(delete_query)
    #sql_conn.commit()
    #print('sql_conn',sql_conn)
    alter_query  = 'ALTER TABLE momentive.unstructured_control_table ALTER COLUMN website_path varchar(265)'
    cursor.execute(alter_query)
    sql_conn.commit()
    if sql_conn is not None: 
      select_query = "select * from momentive.unstructured_control_table where source_type = 'Website'"
      web_df = pd.read_sql(select_query, sql_conn)
      logger.info('Successfully extracted the data of momentive.unstructured_control_table for website from sql server')
    else:
      logger.error('Sql_conn has None value something went wrong in the Sql server connection')
      web_df = pd.DataFrame([], columns=['source_type', 'file_name', 'file_type', 'created', 'updated', 'file_size'])
    return web_df
  except Exception as error:
    logger.error('Something went wrong in the Control_table_website_check function', exc_info=True)

#connecting sql db using pyodbc
def Sql_db_connection(): 
  try:
    server = config.get('sql_db', 'server')
    database = config.get('sql_db', 'database')
    username = config.get('sql_db', 'username')
    password = config.get('sql_db', 'password')
    
    DATABASE_CONFIG = {
      'server': server,
      'database': 'cld-it-dev-pih-db1',
      'username': username,
      'password': password
    }
    driver= "{ODBC Driver 17 for SQL Server}"
    connection_string = 'DRIVER=' + driver + \
                      ';SERVER=' + DATABASE_CONFIG['server'] + \
                      ';PORT=1433' + \
                      ';DATABASE=' + DATABASE_CONFIG['database'] + \
                      ';UID=' + DATABASE_CONFIG['username'] + \
                      ';PWD=' + DATABASE_CONFIG['password'] 


    sql_conn = pyodbc.connect(connection_string)
    logger.info('Successfully connected with the sql serevr ')
    return sql_conn    
  except Exception as e:
    logger.error('Something went wrong in the Sql_db_connection function {} ', exc_info=True) 
    
#Writing metadata json for each product
def metadata_outpath_write(metadata_outpath,product,results,search_area):
    if search_area == 'TDS':
      with open(metadata_outpath + product+ '_TDS' + '.json','w') as js:
                json.dump(results,js)
    else:
      with open(metadata_outpath + product+ '.json','w') as js:
                json.dump(results,js)

#metadata json extraction for each product categpries from momentive home page  
def web_home(web_out_path,web_df):
    try:      
        for product in products:
            logger.info('Extracting the metadata json for {} from momentive Home page(www.momentive.com/en-us) '\
                        .format(product))
            url_handle = urlopen(momen_url[0].format(product))
            headers = url_handle.read().decode("utf-8")
            soup = BeautifulSoup(headers, 'lxml')
            divTag = soup.find_all("div", {"class": "app-content-area inner-wrapper"})
            tdTags = divTag[0].find_all(id="Model")
            headers = json.loads(tdTags[0].text)
            results = headers['Results']
            logger.info('{} related products found in the momentive home page for {}'.format(len(results),product))
            for index in results:
                if index['PageUrl'] is not None:
                    if index['PageUrl'].strip().lower().startswith('https'):
                        if index['TaxonName'] is not None:
                           # Product_category(index,web_out_path,web_df)
                            Related_doc(index,web_out_path,web_df)
                        else:
                            Product_Extract(index,web_out_path,web_df)
                            
                    elif index['PageUrl'].strip().lower().startswith('/en-us/'):
                        if index['TaxonName'] is not None:
                            #Product_category(index,web_out_path,web_df)
                            Related_doc(index,web_out_path,web_df)
                        else:
                            Product_Extract(index,web_out_path,web_df)
                else:
                    if index['TaxonName'] is not None:
                      Related_doc(index,web_out_path,web_df)
            metadata_outpath_write(metadata_outpath,product,results,'home')              
        logger.info('Successfully Completed Fetching all files from Momentive Home Page')  
        
    except Exception as e:
      logger.error('Something went wrong in the web_home {} ', exc_info=True) 
      
#metadata json extraction for each product categpries from TDS page      
def tds_web(web_out_path,web_df):
  try:
    #base_dir = web_out_path.replace('dbfs:','/dbfs') + 'TDS/'
    #path_exists(base_dir)
    base_dir = web_out_path.replace('dbfs:','/dbfs')
    for product in products:
        logger.info('Extracting the metadata json for {} from momentive TDS page(www.momentive.com/en-us/tdssearch)'\
                        .format(product))
        url_handle = urlopen(momen_url[1].format(product))
        headers = url_handle.read().decode("utf-8")
        soup = BeautifulSoup(headers, 'lxml')
        divTag = soup.find_all("div", {"class": "app-content-area inner-wrapper"})
        tdTags = divTag[0].find_all(id="Model")
        headers = json.loads(tdTags[0].text)
        results = headers['Results']
        logger.info('{} related products found in the momentive TDS page for {}'.format(len(results),product))
        for index in results:
            write_flag = ''
            web_inc_flag = ''                                            
            if index['PageUrl'] is not None:
              Product_Extract(index,base_dir,web_df)              
        metadata_outpath_write(metadata_outpath,product,results,'TDS')
  except Exception as e:
    logger.error('Something went wrong in the tds_web {} ', exc_info=True) 

#performing data insertion on unstructured control table:
def web_sql_crud_control_table(sql_conn,web_df):
  try:
    cursor = sql_conn.cursor()
    global metadata_list
    metadata_list = pd.DataFrame(metadata_list,columns=['File_name', 'File_size','Date','website_path'])
    
    #creating data in the conatrol audit table for historical load
    if web_df.empty:
      for i in metadata_list.index:
        insert_query = '''insert into momentive.unstructured_control_table (file_id, category, sharepoint_file_path, azure_blob_file_path, sharepoint_last_modified, is_updated_file, is_relevant, source_type, file_name, file_type, created, updated, file_size, website_path) values ('N/A' , 'N/A' , 'N/A' , 'N/A' , 'N/A' , 0 , 0 ,'Website', '{}' , '{}', '{}', '{}', '{}', '{}') '''.format(metadata_list['File_name'][i][:-4], metadata_list['File_name'][i][-3:], metadata_list['Date'][i], metadata_list['Date'][i], metadata_list['File_size'][i], metadata_list['website_path'][i])
        print(insert_query)
        cursor.execute(insert_query)
        sql_conn.commit()
        logger.info('Successfully inserted the data into unstructred audit table for {}'.format(metadata_list['File_name'][i][:-4]))
    #CRUD operation in the control audit table for incremental load
    
    else:
      for i in metadata_list.index:
        incriment_filt_df = web_df[(web_df['file_name']==metadata_list['File_name'][i][:-4])]       
        #Create query 
        if incriment_filt_df.empty:
          insert_query = '''insert into momentive.unstructured_control_table (file_id, category, sharepoint_file_path, azure_blob_file_path, sharepoint_last_modified, is_updated_file, is_relevant, source_type, file_name, file_type, created, updated, file_size, website_path) values ('N/A' , 'N/A' , 'N/A' , 'N/A' , 'N/A' , 0 , 0 ,'Website', '{}' , '{}', '{}', '{}', '{}', '{}') '''.format(metadata_list['File_name'][i][:-4], metadata_list['File_name'][i][-3:], metadata_list['Date'][i], metadata_list['Date'][i], metadata_list['File_size'][i], metadata_list['website_path'][i])
          cursor.execute(insert_query)
          sql_conn.commit()
          logger.info('Successfully inserted the data into unstructred audit table for {}'.format(metadata_list['File_name'][i][:-4]))
        #Update operation
        else:
          update_query = '''update momentive.unstructured_control_table set updated = '{}', file_size = '{}', is_updated_file = 1 where file_name = '{}' and source_type = '{}' '''.format(metadata_list['Date'][i], metadata_list['File_size'][i], metadata_list['File_name'][i][:-4], 'Website')
          cursor.execute(update_query)
          sql_conn.commit()
          logger.info('Successfully updated the data into unstructred audit table for {}'.format(metadata_list['File_name'][i][:-4]))
      #Delete operations:
      if bool(file_exist_audit_check):
        file_exist_audit_check_set = set(file_exist_audit_check)
        file_list = set(web_df['file_name'].to_list())
        file_difference = list(file_list.difference(file_exist_audit_check_set))
        print('file_difference',file_difference)
        for file_name in file_difference:
          delete_query = '''delete from momentive.unstructured_control_table where source_type = '{}' and file_name ='{}' '''.format('Website',file_name)
          cursor.execute(delete_query)
          sql_conn.commit()
          logger.info('Successfully deleted the data into unstructred audit table for {}'.format(file_name))
  except Exception as error:
    logger.error('Something went wrong in the web_sql_crud_control_table', exc_info=True) 

def web_pdf_extract_text(path, nativeloc, allfiles):
    try:
        allfiles = config.get(path,allfiles)
        path_exists(allfiles)
        native_files = glob.glob(config.get(path, nativeloc) + '*.pdf')
        for files in native_files:
            text=''
            pdf_file = fitz.open(files)
            n_pages = pdf_file.pageCount
            for n in range(n_pages):
                page = pdf_file.loadPage(n)
                text = text + page.getText()            
            basenames=files.split('/')        
            basenames = allfiles + basenames[-1].split('.')[0]
            text_name = basenames.replace("/dbfs","dbfs:") + '.txt'
            dbutils.fs.put(text_name,text,True)
    except Exception as e:
        logger.error(e)
   

def intialize_temp_files(path, temp=None):
    try:
        temp = glob.glob(path + '*.*')  
        if len(temp)==0:
            pass
        else:
            for i in temp:
              i = i.replace("/dbfs","dbfs:")
              dbutils.fs.rm(i)
    except Exception as e:
        logger.error(e)
        
def sil_elast_image_read(files,silicone_elast_path):
  silicone_temp = silicone_elast_path.rsplit('/',1)[0]+'/temp/'
  path_exists(silicone_elast_path.rsplit('/',1)[0]+'/temp/')
  intialize_temp_files(silicone_temp)  
  with Image(filename=files, resolution=300) as img:
    img.units = 'pixelsperinch'
    img.compression_quality = 70
    img.save(filename = silicone_temp + 'out' +'.png') 
  return  silicone_temp 

#FDA Eu profuct extract from silicone elastomer brochure
def fda_eu_product(silicone_elast_path,sql_conn):
  table_dataframe = []  
  for files in glob.glob(silicone_elast_path+'*.pdf'):
    print(files)
    silicone_temp =sil_elast_image_read(files,silicone_elast_path)
    tables = camelot.read_pdf(files,pages='all')

    #itearting the pages in which tables are identified  
    for table in tables:
      #table title will hold the title of table
      table_title =[]
      table_page = table.parsing_report['page'] - 1
      img = cv2.imread(silicone_temp + 'out-{}.png'.format(table_page))
      config1 = ('--psm 6')
      page_table_img_read = pytesseract.image_to_string(img, config=config1)
      table_title.append(page_table_img_read.split('\n')[0].strip())
      print("table_title",table_title)
      #replacing spaces with nan for pandas dataframe to understand
      table.df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
      #dropping the null lines
      table.df.dropna(how ='all',inplace=True)
      #resting the index of dataframe
      table.df.reset_index(drop=True,inplace=True)
      # getting record only if it contains +•-
      if not table.df.empty and table.shape[1] == 1:
          rgx = re.compile(r'[+•-]',re.I)
          table_fmt = table.df[(table.df[0].astype(str).str.contains(rgx))] 
          table_dataframe.append(pd.DataFrame(table_title))
          table_dataframe.append(table_fmt)
      elif not table.df.empty and table.shape[1]<=3:
          rgx = re.compile(r'[+•-]',re.I)
          table_fmt = table.df[(table.df[2].astype(str).str.contains(rgx))|(table.df[0].astype(str).str.contains(rgx))] 
          table_dataframe.append(pd.DataFrame(table_title))
          table_dataframe.append(table_fmt)
      elif not table.df.empty and table.shape[1]>3:  
          rgx = re.compile(r'[+•-]',re.I)
          table_fmt = table.df[(table.df[2].astype(str).str.contains(rgx))|(table.df[0].astype(str).str.contains(rgx)) | (table.df[3].astype(str).str.contains(rgx))]        
          table_dataframe.append(pd.DataFrame(table_title))
          table_dataframe.append(table_fmt)
  df = pd.concat(table_dataframe)
  df.reset_index(drop=True,inplace=True)

  #splitting the first column data if all the column data concatanated in it with the help of \n present in the column   
  for i in df.index:
      count=0
      if not pd.isnull(df.iloc[i,0]):
          #print('rmmaa')
          if '\n' in df.loc[i,0] and ('+' in df.loc[i,0] or '•' in df.loc[i,0] or '-' in df.loc[i,0]):
              #print(df.loc[i,0])
              sp=df.loc[i,0].split('\n')
              if sp[0] == 'ü':
                  sp[0],sp[1]= sp[1],sp[0]
              sp.insert(1,' ')
             # print('sp',sp)
              for j in sp:
                  try:
                      if not j.isspace():
                          #print(i,count)
                          df.iat[i,count]=str(j)
                      else:
                          pass
                      count += 1   
                  except IndexError:
                      pass
                  except ValueError:
                      pass
  #print(df)
 # df.to_csv(silicone_elast_path.rsplit('/',1)[0].replace('dbfs:','/dbfs') +'/silicone_elastomer_table_check.csv')            
  final_dict={
          'EU':[],
          'FDA' : []
          }

  liquid_health_flag = ''
  hcr_portfolio_flag = ''
  hcr_silplus_flag = ''
  genral_spec_lubric_bonding_fluro_flag = ''
  hcr_portfolio_count = 0


  for i in df.index:
      # Liquid Silicone Rubber Grades General Purpose or Liquid Silicone Rubber Grades Specia1t1es or Liquid Silicone Rubber Grades Se|f—Lubricating or Liquid SI cone Rubber Grades Self-Bonding orLiquid Silicone Rubber Grades Fluorosilicones or Liquid SI cone Rubber Grades Self-Bonding
       if not pd.isnull(df[0][i]) and (df[0][i].strip() == 'Liquid Silicone Rubber Grades General Purpose' or df[0][i].strip() == 'Liquid Silicone Rubber Grades Specia1t1es' or df[0][i].strip() == 'Liquid Silicone Rubber Grades Se|f—Lubricating' or df[0][i].strip() == 'Liquid SI cone Rubber Grades Self-Bonding' or df[0][i].strip() == 'HCR General Purpose' or df[0][i].strip() == 'HCR Fluorosilicones' or df[0][i].strip() == 'Liquid Silicone Rubber Grades High Voltage Industry'):
           genral_spec_lubric_bonding_fluro_flag = 's'
           liquid_health_flag = ''
           hcr_portfolio_flag = ''
           hcr_silplus_flag = ''

       #  Liquid Silicone Rubber Grades Healthcare     
       if not pd.isnull(df[0][i]) and (df[0][i].strip() == 'Liquid Silicone Rubber Grades Healthcare' or df[0][i].strip() == 'Liquid Silicone Rubber Grades Hea\thcare'):
           liquid_health_flag = 's'
           genral_spec_lubric_bonding_fluro_flag = ''
           hcr_portfolio_flag = ''
           hcr_silplus_flag = ''
       # HCR Addition Curing Portfolio    
       if not pd.isnull(df[0][i]) and df[0][i].strip() == 'HCR Addition Curing Portfolio':   
           hcr_portfolio_flag = 's'
           genral_spec_lubric_bonding_fluro_flag = ''
           liquid_health_flag = ''
           hcr_silplus_flag = ''
           hcr_portfolio_count += 1
       #HCR Silplus products   
       if not pd.isnull(df[0][i]) and (df[0][i].strip() == 'HCR Silplus" Products' or df[0][i].strip() == '. *'):   
           hcr_silplus_flag = 's'
           genral_spec_lubric_bonding_fluro_flag = ''
           hcr_portfolio_flag = ''
           liquid_health_flag = ''
      # Liquid Silicone Rubber Grades General Purpose or Liquid Silicone Rubber Grades Specia1t1es or Liquid Silicone Rubber Grades Se|f—Lubricating or Liquid SI cone Rubber Grades Self-Bonding orLiquid Silicone Rubber Grades Fluorosilicones or Liquid SI cone Rubber Grades Self-Bonding
       if  genral_spec_lubric_bonding_fluro_flag == 's':   
           if not pd.isnull(df[2][i]):
               if (df[2][i].strip() == '+' or df[2][i].strip() == '•' or df[2][i].strip() == 'â€¢'):
                   if '\n' in df[0][i]:
                      value = str(df[0][i]).split('\n') 
                      final_dict['EU'].append(value[1])
                   else:
                      final_dict['EU'].append(df[0][i])

           if not pd.isnull(df[3][i]):
              if (df[3][i].strip() == '+' or df[3][i].strip() == '•' or df[3][i].strip() == 'â€¢'):
                  if '\n' in df[0][i]:
                      value = str(df[0][i]).split('\n') 
                      final_dict['FDA'].append(value[1])
                  else:
                      final_dict['FDA'].append(df[0][i])

       #  Liquid Silicone Rubber Grades Healthcare  
       if liquid_health_flag == 's':
           if not pd.isnull(df[4][i]):
              if (df[4][i].strip() == '+' or df[4][i].strip() == '•' or df[4][i].strip() == 'â€¢'):
                  if '\n' in df[0][i]:
                      value = str(df[0][i]).split('\n') 
                      final_dict['EU'].append(value[1])
                  else:
                      final_dict['EU'].append(df[0][i])

           if not pd.isnull(df[5][i]):
              if (df[5][i].strip() == '+' or df[5][i].strip() == '•' or df[5][i].strip() == 'â€¢'):
                  if '\n' in df[0][i]:
                      value = str(df[0][i]).split('\n') 
                      final_dict['FDA'].append(value[1])
                  else:
                      final_dict['FDA'].append(df[0][i])

      # HCR Addition Curing Portfolio
       if hcr_portfolio_flag == 's':
           if hcr_portfolio_count == 1:
               if not pd.isnull(df[5][i]):
                  if (df[5][i].strip() == '+' or df[5][i].strip() == '•' or df[5][i].strip() == 'â€¢'):
                      if '\n' in df[0][i]:
                          value = str(df[0][i]).split('\n') 
                          final_dict['EU'].append(value[1])
                      else:
                          final_dict['EU'].append(df[0][i])

               if not pd.isnull(df[6][i]):
                  if (df[6][i].strip() == '+' or df[6][i].strip() == '•' or df[6][i].strip() == 'â€¢'):
                      if '\n' in df[0][i]:
                          value = str(df[0][i]).split('\n') 
                          final_dict['FDA'].append(value[1])
                      else:
                          final_dict['FDA'].append(df[0][i])
           else:
               if not pd.isnull(df[3][i]):
                  if (df[3][i].strip() == '+' or df[3][i].strip() == '•' or df[3][i].strip() == 'â€¢'):
                     if '\n' in df[0][i]:
                          value = str(df[0][i]).split('\n') 
                          final_dict['EU'].append(value[1])
                     else:
                          final_dict['EU'].append(df[0][i])

               if not pd.isnull(df[4][i]):
                  if (df[4][i].strip() == '+' or df[4][i].strip() == '•' or df[4][i].strip() == 'â€¢'):
                      if '\n' in df[0][i]:
                          value = str(df[0][i]).split('\n') 
                          final_dict['FDA'].append(value[1])
                      else:
                          final_dict['FDA'].append(df[0][i])

      #HCR Silplus products
       if hcr_silplus_flag == 's':
           if not pd.isnull(df[3][i]):
               if (df[3][i].strip() == '+' or df[3][i].strip() == '•' or df[3][i].strip() == 'â€¢'):
                   if '\n' in df[0][i]:
                      value = str(df[0][i]).split('\n') 
                      final_dict['EU'].append(value[1])
                   else:
                      final_dict['EU'].append(df[0][i])

           if not pd.isnull(df[4][i]):
              if (df[4][i].strip() == '+' or df[4][i].strip() == '•' or df[4][i].strip() == 'â€¢'):
                  if '\n' in df[0][i]:
                      value = str(df[0][i]).split('\n') 
                      final_dict['FDA'].append(value[1])
                  else:
                      final_dict['FDA'].append(df[0][i])
  df1 = pd.concat([pd.Series(v, name=k) for k, v in final_dict.items()], axis=1) 
  df1[df1 == '•'] = np.NaN
  df1[df1 == '+'] = np.NaN
  df1.dropna(how ='any',inplace=True)
  df1.reset_index(drop=True,inplace=True) 
  df1.to_csv(silicone_elast_path.rsplit('/',1)[0].replace('dbfs:','/dbfs') +'/silicone_elastomer_table_check.csv')
  cursor = sql_conn.cursor()
  for index in df1.index:
     if df1['EU'][index]:
       eu_product = df1['EU'][index]
     else:
       eu_product = 'null'
     if df1['FDA'][index]:
       fda_product = df1['FDA'][index]
     else:
       fda_product = 'null' 
        
     insert_query = "insert into momentive.silicone_elastomer_table (us_fda, eu_fda) values ('{}','{}')".format(fda_product,eu_product)
     cursor.execute(insert_query)
     sql_conn.commit()
    
if __name__ == '__main__':     
    try:
      logger.info('Beginning of file extraction from momentive websites')
      #if not os.path.exists(web_out_path):
      path_exists(web_out_path)
      path_exists(metadata_outpath)
      sql_conn = Sql_db_connection()
      web_df = Control_table_website_check(sql_conn)
      web_home(web_out_path,web_df)
      tds_web(web_out_path,web_df)
      web_sql_crud_control_table(sql_conn,web_df)
      
      if 'Silicone Elastomers Brochure.pdf' in os.listdir(web_out_path):
        path_exists(silicone_elast_path)
        web_elast_path = os.path.join(web_out_path,'Silicone Elastomers Brochure.pdf')
        dbutils.fs.cp(web_elast_path.replace("/dbfs","dbfs:"),silicone_elast_path.replace("/dbfs","dbfs:"))
        fda_eu_product(silicone_elast_path,sql_conn)
    except Exception as e:
      logger.error('Something went wrong in the main ',exc_info=True)
    finally:
      df1 = pd.DataFrame(metadata_list,columns=['File_name', 'File_size','Date'])


# COMMAND ----------

# MAGIC %sh
# MAGIC cat /databricks/driver/momentive_web_error.log

# COMMAND ----------

# COMMAND ----------

cp /databricks/driver/momentive_web_error.log /dbfs/mnt/web-files/
# COMMAND ----------

# MAGIC %sh
# MAGIC cat /databricks/driver/momentive_web_error.log

# COMMAND ----------

config = configparser.ConfigParser()
config.read("/dbfs/mnt/python/configuration/config.ini")
def path_exists(file_path):
  try:
    if file_path is not None and file_path != '':
      dbutils.fs.rm(file_path.replace("/dbfs",""),True)
      dbutils.fs.mkdirs(file_path.replace("/dbfs","dbfs:"))
      logger.info('Successfully created website staging blob stoarge {} '.format(file_path))
    else:
      logger.error('Website output path given is None or empty ')    
  except Exception as e:
      logger.error('Something went wrong in the path_exists function ', exc_info=True) 

def split_File_on_Type(files_list,file_path):
  if bool(files_list):
    path_exists(file_path)
    for file in files_list:
      file=file.replace("/dbfs","dbfs:")
      file_loc=file_path.replace("/dbfs","dbfs:")
      dbutils.fs.cp(file, file_loc) 
  
  
#splitting th file based on extension:
web_storage = config.get('web', 'web_storage')
file_type = config.get('web', 'file_type')
pdf_path = config.get('web', 'pdf_path')
path_exists(file_type)
pdf_files =  glob.glob(web_storage+'*.pdf')
doc_files =  glob.glob(web_storage+'*.doc')
docx_files = glob.glob(web_storage+'*.docx')
ppt_files =  glob.glob(web_storage+'*.ppt')
pptx_files = glob.glob(web_storage+'*.pptx')
web_cat_files = glob.glob(web_storage+'*.txt')
split_File_on_Type(pdf_files,pdf_path)

# COMMAND ----------

import pandas as pd
DATABASE_CONFIG = {
    'server': 'pih.database.windows.net',
    'database': 'cld-it-dev-pih-db1',
    'username': 'PIH-admin',
    'password': 'Password@1234Momentive!'
}

import pyodbc
#import config
import pandas as pd
import traceback

driver= "{ODBC Driver 17 for SQL Server}"
connection_string = 'DRIVER=' + driver + \
                    ';SERVER=' + DATABASE_CONFIG['server'] + \
                    ';PORT=1433' + \
                    ';DATABASE=' + DATABASE_CONFIG['database'] + \
                    ';UID=' + DATABASE_CONFIG['username'] + \
                    ';PWD=' + DATABASE_CONFIG['password'] 
                    
try:
    sql_conn = pyodbc.connect(connection_string)
    cursor = sql_conn.cursor()
except Exception as error:
#    print("    \u2717 accesing {} has error".format(config.DATABASE_CONFIG['database']))
    print("    \u2717 error message: {}".format(error))
    # I found that traceback prints much more detailed error message
    traceback.print_exc()
create_query ='''CREATE TABLE momentive.product_inscope (nam_prod varchar(8000),
bdt varchar(8000),
cas_no varchar(8000),
spec_id varchar(8000),
material_no varchar(8000))
'''




cursor.execute(create_query)
sql_conn.commit()
# def web_insert_audit_table():
#   metadata_list = pd.read_csv('/dbfs/mnt/web-files/output.csv')
#   select_query = "select * from momentive.unstructured_control_table where source_type = 'Website'"
#   df1 = pd.read_sql(select_query, sql_conn)

#   if df1.empty:
#    for i in metadata_list.index:
#      insert_query = '''insert into momentive.unstructured_control_table (file_id, category, sharepoint_file_path, azure_blob_file_path, sharepoint_last_modified, is_updated_file, is_relevant, source_type, file_name, file_type, created, updated, file_size) values ('N/A' , 'N/A' , 'N/A' , 'N/A' , 'N/A' , 0 , 0 ,'Website', '{}' , '{}', '{}', '{}', '{}')'''.format(metadata_list['File_name'][i][:-4], metadata_list['File_name'][i][-3:], metadata_list['Date'][i], metadata_list['Date'][i], metadata_list['File_size'][i])
#      cursor.execute(insert_query)
#      sql_conn.commit()
#   else:
#       for i in metadata_list.index:
#         #print(metadata_list['File_name'][i][:-4])
#         incriment_filt_df = df1[(df1['file_name']==metadata_list['File_name'][i][:-4])]
#         if incriment_filt_df.empty:
#           print(incriment_filt_df)
#           insert_query = '''insert into momentive.unstructured_control_table (file_id, category, sharepoint_file_path, azure_blob_file_path, sharepoint_last_modified, is_updated_file, is_relevant, source_type, file_name, file_type, created, updated, file_size) values ('N/A' , 'N/A' , 'N/A' , 'N/A' , 'N/A' , 0 , 0 ,'Website', '{}' , '{}', '{}', '{}', '{}')'''.format(metadata_list['File_name'][i][:-4], metadata_list['File_name'][i][-3:], metadata_list['Date'][i], metadata_list['Date'][i], metadata_list['File_size'][i])
#           print(insert_query)
#           cursor.execute(insert_query)
#           sql_conn.commit()
#         else:
#           update_query = '''update momentive.unstructured_control_table set updated = '{}', file_size = '{}', is_updated_file = 1 where file_name = '{}' and source_type = '{}' '''.format(metadata_list['Date'][i], metadata_list['File_size'][i], metadata_list['File_name'][i][:-4], 'Website')
#           print(update_query)
#           cursor.execute(update_query)
#           sql_conn.commit()
        
     #print(metadata_list['File_name'][i])
  #else:
    #if 
#web_insert_audit_table()

# COMMAND ----------

alter_qyery= 'ALTER TABLE momentive.unstructured_control_table ADD file_size int'
sql_conn.execute(alter_qyery)

# COMMAND ----------

import requests 
a='https://www.momentive.com/docs/default-source/productdocuments/silopren-lsr-7180/silopren-lsr-7180-marketing-bulletin.pdf?sfvrsn=5f7e708f_20'
r = requests.get(a)
print(r.headers)

# COMMAND ----------

a='''Key
FDA_Value
FDA_Date
FDA_FileName
FDA_Filepath
EU_Value
EU_FileName
EU_Filepath
POTENTIAL_Value
Substance
Classification
Reason Code
Source (Legal requirements, regulations)
Reporting threshold (0.1% unless otherwise stated)
Chemical
Type of Toxicity
Listing Mechanism
NSRL or MADL
SYN_PATH
MNF_PATH
Test Article Number
Test Article Description
Study Title
Report Date
Product-Commerical Name
Studies
Status
Comments
AVAIL_VALUE
Latam_Country
Registered Name
Date Granted
Date of Expiry
Registration Holder
Registration Certificate
EPA Inert Product Listing
CA DPR 
CPDA
WSDA
OMRI
OMRI Renewal Date
Canada OMRI
PMRA
Eu_Country
Holder
Registration
Expiry
Status
Certificate
CHEM_VALUE
Molecular_Weight_Value
Molecular_formula_value
HVT_MTS_Value'''
a=a.split('\n')
for i in a:
  print(i + ' varchar(8000),')

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://momentive-sources-pih@clditdevstoragepih.blob.core.windows.net",
  mount_point = "/mnt/momentive-sources-pih",
  extra_configs = {"fs.azure.account.key.clditdevstoragepih.blob.core.windows.net":"nXtHsqdCE9zBg3wEkW1Upn7h6bS6hUJEpg8dBRA9RipfcCC1Eji8aVCG+PP9eF0xOrvHE3w1QBaiMBGvNrXTJw=="}) 

# COMMAND ----------

