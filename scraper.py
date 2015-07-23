# -*- coding: utf-8 -*-
import os
import re
import requests
import scraperwiki
import urllib2
#from datetime import datetime
from bs4 import BeautifulSoup
import datetime
from dateutil.rrule import rrule, MONTHLY
import time
from time import strptime, strftime
#from datetime import date


# Set up variables
entity_id = "E0501_PCC_gov"
url = "http://data.peterborough.gov.uk/View/commercial-activities/transparency-code-payments-over-500"
errors = 0
start_date = datetime.date(2014,1,1).strftime('%d/%m/%Y')
std = datetime.datetime(2014,1,1)
end_date = datetime.date(2015,7,22).strftime('%d/%m/%Y')
edd = datetime.datetime(2015,7,22)
user_agent = {'User-agent': 'Mozilla/5.0'}
dates_csv = [dt.strftime('%m %Y') for dt in rrule(MONTHLY, dtstart=std, until=edd)]

data = {'OrderByColumn':'[BodyName]',
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

# Set up functions
def validateFilename(filename):
    filenameregex = '^[a-zA-Z0-9]+_[a-zA-Z0-9]+_[a-zA-Z0-9]+_[0-9][0-9][0-9][0-9]_[0-9][0-9]$'
    dateregex = '[0-9][0-9][0-9][0-9]_[0-9][0-9]'
    validName = (re.search(filenameregex, filename) != None)
    found = re.search(dateregex, filename)
    if not found:
        return False
    date = found.group(0)
    year, month = int(date[:4]), int(date[5:7])
    now = datetime.datetime.now()
    validYear = (2000 <= year <= now.year)
    validMonth = (1 <= month <= 12)
    if all([validName, validYear, validMonth]):
        return True
def validateURL(url, data):
    try:
        r = requests.post(url, data = data, allow_redirects=True, timeout=20)
        count = 1
        while r.status_code == 500 and count < 4:
            print ("Attempt {0} - Status code: {1}. Retrying.".format(count, r.status_code))
            count += 1
            r = requests.post(url, data=data, allow_redirects=True, timeout=20)
        sourceFilename = r.headers.get('Content-Disposition')

        if sourceFilename:
            ext = os.path.splitext(sourceFilename)[1].replace('"', '').replace(';', '').replace(' ', '')
        else:
            ext = os.path.splitext(url)[1]
        validURL = r.status_code == 200
        validFiletype = ext in ['.csv', '.xls', '.xlsx']
        return validURL, validFiletype
    except:
        raise
def convert_mth_strings ( mth_string ):

    month_numbers = {'JAN': '01', 'FEB': '02', 'MAR':'03', 'APR':'04', 'MAY':'05', 'JUN':'06', 'JUL':'07', 'AUG':'08', 'SEP':'09','OCT':'10','NOV':'11','DEC':'12' }
    #loop through the months in our dictionary
    for k, v in month_numbers.items():
#then replace the word with the number

        mth_string = mth_string.replace(k, v)
    return mth_string
# pull down the content from the webpage
html = requests.get(url, headers = user_agent)
soup = BeautifulSoup(html.text)
# find all entries with the required class
links = soup.find('a', attrs = {'id':'downloadData'})

for date_csv in dates_csv:
    csvfile = date_csv.split(' ')
    csvYr = csvfile[-1]
    csvMth = csvfile[0]
    csvMth = convert_mth_strings(csvMth.upper())
    filename = entity_id + "_" + csvYr + "_" + csvMth
    todays_date = str(datetime.datetime.now())
    file_url = url
    validFilename = validateFilename(filename)
    validURL, validFiletype = validateURL(file_url, data)
    if not validFilename:
        print filename, "*Error: Invalid filename*"
        print file_url
        errors += 1
        continue
    if not validURL:
        print filename, "*Error: Invalid URL*"
        print file_url
        errors += 1
        continue
    if not validFiletype:
        print filename, "*Error: Invalid filetype*"
        print file_url
        errors += 1
        continue
    scraperwiki.sqlite.save(unique_keys=['f'], data={"l": file_url, "f": filename, "d": todays_date })
    print filename
if errors > 0:
    raise Exception("%d errors occurred during scrape." % errors)
