from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import csv

# Selenium web driver to start Chrome in background
options = Options()
options.add_argument('--headless')
driver = "drivers/chromedriver.exe"

# browser session to grab html for each specialty page
service = Service(driver)
browser = webdriver.Chrome(service=service, options=options)
url = "https://boston.devicetalks.com/speakers/"
browser.get(url)
html = browser.page_source
browser.close()
soup_speakers = BeautifulSoup(html, 'html.parser')

name_title_company = []

for name in soup_speakers.find_all(class_ = "speaker_info_wrapper"):
    speaker_name = name.find('h4').text
    title_company = name.find(class_ = "speaker_desc").text
    if '|' in title_company:
        split_on = '|'
        job_title = title_company.split(sep=split_on)[0].strip()
        company_name = title_company.split(sep=split_on)[1].strip()
    elif ',' in title_company:
        split_on = ','
        job_title = title_company.split(sep=split_on)[0].strip()
        company_name = title_company.split(sep=split_on)[1].strip()
    elif 'I' in title_company:
        split_on = 'I'
        job_title = title_company.split(sep=split_on)[0].strip()
        company_name = title_company.split(sep=split_on)[1].strip()
    else:
        job_title = title_company
        company_name = ''
    name_title_company.append((speaker_name, job_title, company_name))

with open("device_talks_speakers.csv", "w") as f:
    csv_writer = csv.writer(f)
    for speaker in name_title_company:
        csv_writer.writerow(speaker)
    f.close()

