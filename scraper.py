# -*- coding: utf-8 -*-

#### IMPORTS 1.0

import os
import re
import scraperwiki
import datetime
from dateutil.rrule import rrule, MONTHLY
from bs4 import BeautifulSoup

#### FUNCTIONS 1.2

import requests   # import requests for validating url and making post requests

def validateFilename(filename):
    filenameregex = '^[a-zA-Z0-9]+_[a-zA-Z0-9]+_[a-zA-Z0-9]+_[0-9][0-9][0-9][0-9]_[0-9QY][0-9]$'
    dateregex = '[0-9][0-9][0-9][0-9]_[0-9QY][0-9]'
    validName = (re.search(filenameregex, filename) != None)
    found = re.search(dateregex, filename)
    if not found:
        return False
    date = found.group(0)
    now = datetime.datetime.now()
    year, month = date[:4], date[5:7]
    validYear = (2000 <= int(year) <= now.year)
    if 'Q' in date:
        validMonth = (month in ['Q0', 'Q1', 'Q2', 'Q3', 'Q4'])
    elif 'Y' in date:
        validMonth = (month in ['Y1'])
    else:
        try:
            validMonth = datetime.datetime.strptime(date, "%Y_%m") < now
        except:
            return False
    if all([validName, validYear, validMonth]):
        return True


def validateURL(url):
        import urllib, urllib2
#     try:
        data = urllib.urlencode(datadict)
        req = urllib2.Request(url, data)
        response = urllib2.urlopen(req)
        print response.info().getheader('Content-Type')
    #try:
      
   # except:
   #     print ("Error validating URL.")
   #     return False, False

def validate(filename, file_url):
    validFilename = validateFilename(filename)
    validURL, validFiletype = validateURL(file_url)
    if not validFilename:
        print filename, "*Error: Invalid filename*"
        print file_url
        return False
    if not validURL:
        print filename, "*Error: Invalid URL*"
        print file_url
        return False
    if not validFiletype:
        print filename, "*Error: Invalid filetype*"
        print file_url
        return False
    return True


def convert_mth_strings ( mth_string ):
    month_numbers = {'JAN': '01', 'FEB': '02', 'MAR':'03', 'APR':'04', 'MAY':'05', 'JUN':'06', 'JUL':'07', 'AUG':'08', 'SEP':'09','OCT':'10','NOV':'11','DEC':'12' }
    for k, v in month_numbers.items():
        mth_string = mth_string.replace(k, v)
    return mth_string


#### VARIABLES 1.0

entity_id = "E0501_PCC_gov"
url = "http://data.peterborough.gov.uk/View/commercial-activities/transparency-code-payments-over-500"
errors = 0
start_date = datetime.date(2014,1,1).strftime('%d/%m/%Y')
std = datetime.datetime(2014,1,1)
end_date = datetime.datetime.now().strftime('%d/%m/%Y')
edd = datetime.datetime.now()
user_agent = {'User-agent': 'Mozilla/5.0'}
dates_csv = [dt.strftime('%m %Y') for dt in rrule(MONTHLY, dtstart=std, until=edd)]

datadict = {'OrderByColumn':'[BodyName]',
'OrderByDirection':'ASC',
'Download':'csv',
'radio':'on',
'chartType':'table',
'filter[0].ColumnToSearch':'Date',
'filter[0].SearchOperator':'contains',
'filter[0].SearchText':'',
'filter[0].SearchOperatorNumber':'greaterthan',
'filter[0].SearchNumber':'',
'filter[0].From':'{}'.format(start_date),
'filter[0].To':'{}'.format(end_date),
'VisibleColumns':'0_Amount',
'VisibleColumns':'1_BodyName',
'VisibleColumns':'2_CapitalorRevenue',
'VisibleColumns':'3_Date',
'VisibleColumns':'4_ExpensesType',
'VisibleColumns':'5_MerchantCategory',
'VisibleColumns':'6_ServiceAreaCategorisation',
'VisibleColumns':'7_ServiceDivisionCategorisation',
'VisibleColumns':'8_SupplierName',
'VisibleColumns':'9_TransactionNumber',
'getVisualisationData':	'false',
'xAxis':'Text#BodyName',
'yAxis':'Number#TransactionNumber',
'yAxisAggregate':'sum',
'chartCurrentPage':'1',
'chartNumberToShow':'10',
'numberToShow':	'10',
'CurrentPage':	'1',
'PageNumber':''	}

data = []


#### READ HTML 1.2


html = requests.get(url, headers = user_agent)     # using requests for making post requests
soup = BeautifulSoup(html.text, 'lxml')


#### SCRAPE DATA

links = soup.find('a', attrs = {'id':'downloadData'})
csvMth = 'Y1'
csvYr = '2017'
csvMth = convert_mth_strings(csvMth.upper())
data.append([csvYr, csvMth, url])

#### STORE DATA 1.0

for row in data:
    csvYr, csvMth, url = row
    filename = entity_id + "_" + csvYr + "_" + csvMth
    todays_date = str(datetime.datetime.now())
    file_url = url.strip()


    valid = validate(filename, file_url)

    if valid == True:
        scraperwiki.sqlite.save(unique_keys=['f'], data={"l": file_url, "f": filename, "d": todays_date })
        print filename
    else:
        errors += 1

if errors > 0:
    raise Exception("%d errors occurred during scrape." % errors)

#### EOF
