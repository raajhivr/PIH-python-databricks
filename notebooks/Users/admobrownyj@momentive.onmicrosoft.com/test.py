# Databricks notebook source
import pandas as pd

# COMMAND ----------

import glob
import pytesseract as pyt
import logging
import datetime
import configparser
import shutil
import fitz
import os
from PIL import Image, ImageFilter
from wand.image import Image as wimage
import PIL
import docx
import pptx
import pandas as pd
import PyPDF2 
import openpyxl
import csv
import re
import nltk
from nltk import ngrams
from outlook_msg import Message
import json
import pyodbc
import camelot

# COMMAND ----------

# MAGIC %sh
# MAGIC sed 's/rights="none" pattern="PDF"/rights="read|write" pattern="PDF"/' /etc/ImageMagick-6/policy.xml  

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/update-policy.sh","""sed 's/rights="none" pattern="PDF"/rights="read|write" pattern="PDF"/' /etc/ImageMagick-6/policy.xml""", True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /etc/ImageMagick-6/policy.xml 

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/update-tesseract.sh","""
#!/bin/bash
sudo add-apt-repository -y ppa:alex-p/tesseract-ocr
sudo apt update
sudo apt-get -q -y install tesseract-ocr""", True)

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/install-pyodbc.sh","""
#!/bin/bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
apt-get -y install unixodbc-dev
sudo apt-get install python3-pip -y
pip3 install --upgrade pyodbc
/databricks/python/bin/pip install pyodbc""", True) 

# COMMAND ----------

import outlook_msg

# COMMAND ----------

import fitz

# COMMAND ----------

import glob

# COMMAND ----------

path = '/dbfs/mnt/test-pih/'
files = glob.glob(path + '*.pdf')
for file in files:
  text=''
  pdf_file = fitz.open(file)
  n_pages = pdf_file.pageCount
  for n in range(n_pages):
      page = pdf_file.loadPage(n)
      text = text + page.getText()
  basenames=file.split('/')            
  basenames= path+(basenames[-1].split('.'))[0]
  text_name = basenames.replace("/dbfs","dbfs:") + '.txt'
  dbutils.fs.put(text_name,text,True)

# COMMAND ----------

path = '/dbfs/mnt/test-pih/'
files = glob.glob(path + 'test/' + '*.txt')
for file in files:
  with open(file, encoding="utf-8") as f:
      block_str = f.read()
      f.close()

# COMMAND ----------

import re
regex1 = re.compile(r'((\d+/+\d+))', re.I)

li = []
for match in re.finditer(regex1,block_str.lower()):
  product_flag = ''
  li.append(match.group()) 
dat = '01/'+ li[-1]

# COMMAND ----------

from datetime import datetime
new1 = []
new =str(datetime.strptime(dat, '%d/%m/%y'))
new1.append(new)
import pandas as pd
df = pd.DataFrame(new1)
df['Date'] = 'Date'
df.columns = ['Value', 'Date']
df = df.loc[:,['Date', 'Value']]

# COMMAND ----------

data = pd.read_csv('/dbfs/mnt/test-pih/python/relevant_data_files/relevant_data.csv')

# COMMAND ----------

data.drop(columns=['Product_category', 'Component'], inplace=True)

# COMMAND ----------

data.head(2)

# COMMAND ----------

data1 = pd.read_csv('/dbfs/mnt/test-pih/')