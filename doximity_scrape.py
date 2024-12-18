from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
import pandas as pd

# Selenium web driver to start Chrome in background
options = Options()
options.add_argument('--headless')
driver = "drivers/chromedriver.exe"

# browser session to grab html for each specialty page
service = Service(driver)
browser = webdriver.Chrome(service=service, options=options)
url = "https://www.doximity.com/residency/specialties"
browser.get(url)
html = browser.page_source
browser.close()
soup_specialties = BeautifulSoup(html, 'html.parser')
links_specialties = []
# store in tuple with url & specialty
for link in soup_specialties.find_all(class_ = "residency-specialties-directory-list-item-link"):
    links_specialties.append((link["href"], link.text.strip()))

# Create list of programs per specialty HTML from url, store urls & specialties in tuple and close browser
service = Service(driver)
browser = webdriver.Chrome(service=service, options=options)
specialty_programs_html = []
for link in links_specialties:
    url = "https://www.doximity.com" + link[0]
    browser.get(url)
    html = browser.page_source
    specialty_programs_html.append((link[1], html))
browser.close()

soup_specialties_list = []
for spec in specialty_programs_html:
    html = BeautifulSoup(spec[1], 'html.parser')
    spec = spec[0]
    soup_specialties_list.append((spec, html))

# Parse html and get links to individual pages
url_start = "/residency/programs/"
urls = []
for spec_page in soup_specialties_list:
    links = [link["href"] for link in spec_page[1].find_all("a") if link["href"].startswith(url_start)]
    specialty = spec_page[0]
    urls.append((specialty, links))

# Start new browser session for individual program scraping
service = Service(driver)
browser = webdriver.Chrome(service=service, options=options)

# Scrape details for each program page and add to list as tuple w/ specialty & html
url_header = "https://www.doximity.com"
all_programs_html_list = []

for i, specialty_tuple in enumerate(urls):
    print(f"Getting {specialty_tuple[0]} - {len(specialty_tuple[1])} urls")
    for j, program_url in enumerate(specialty_tuple[1]):
        url_program = url_header + program_url
        browser.get(url_program)
        html_program = browser.page_source
        all_programs_html_list.append((specialty_tuple[0], url_program, html_program))

browser.close()

# Empty list to hold beautiful soup parsed html in tuple w/ specialty & url
all_programs_soup_list = []
for html in all_programs_html_list:
    soup_program = BeautifulSoup(html[2], 'html.parser')
    all_programs_soup_list.append((html[0], html[1], soup_program))

# empty list to hold each program dictionary
program_dict_list = []

# Regex2 for email parsing
regex2 = re.compile(r'mailto:([^\?]*)')

# Loop through list to get details from html
for program in all_programs_soup_list:
# empty dict to hold details
    program_dict = {}
# add Specialty & data source url to dictionary
    program_dict["ProgramSpecialty"] = program[0]
    program_dict["DataSourceURL"] = program[1]
    print(program_dict["ProgramSpecialty"])
    print(program_dict["DataSourceURL"])
# get name & url of program
    try:
        soup_program_name = program[2].find(class_ = "residency-program-hero-title").text
        print(soup_program_name)
    except AttributeError:
        soup_program_name = ''
    try:
        soup_program_url = program[2].find(class_ = "residency-program-website-url").text
        print(soup_program_url)
    except AttributeError:
        soup_program_url = ''
    try:
        soup_program_details = program[2].find(class_ = "residency-program-hero-subtitle").text.strip()
        print(soup_program_details)
    except AttributeError:
        soup_program_details = ''

    program_dict["ProgramName"] = soup_program_name.strip()
    program_dict["ProgramURL"] = soup_program_url.strip()
    program_dict["ProgramDetails"] = soup_program_details

    try:
        soup_program_metric_numbers = [numbers.text for numbers in program[2].find_all(class_ = "metric-box-number")]
        soup_program_metric_labels = [labels.text for labels in program[2].find_all(class_="metric-box-label")]
        for index, value in enumerate(soup_program_metric_numbers):
            dict_label = soup_program_metric_labels[index].strip()
            program_dict[dict_label] = value.strip()
    except AttributeError:
        continue

# get name/details of contact
    try:
        soup_contact_name = [name.text.strip() for name in program[2].find_all(class_ = "residency-program-contacts-coordinator-name")]
        print(soup_contact_name)
    except AttributeError:
        soup_contact_name = ['']
    try:
        soup_contact_phone = [phone.text.strip() for phone in program[2].find_all(class_ =  "residency-program-contacts-coordinator-phone")]
        print(soup_contact_phone)
    except AttributeError:
        soup_contact_phone = ['']
    try:
        soup_contact_email = [email['href'] for email in program[2].find_all(class_ = 'residency-program-contacts-coordinator-email')]
# regex2 match for each email in list
        soup_contact_email_use = []
        for email in soup_contact_email:
            regex_output = regex2.match(email).group()
            soup_contact_email_use.append(regex_output[7:])
        print(soup_contact_email_use)
    except AttributeError:
        soup_contact_email_use = ['']
    program_dict["ProgramContactName"] = soup_contact_name
    program_dict["ProgramContactPhone"] = soup_contact_phone
    program_dict["ProgramContactEmail"] = soup_contact_email_use
    program_dict_list.append(program_dict)

# create data frame from list of dictionaries
program_df = pd.DataFrame(program_dict_list).fillna('')
# use series explode to create rows for each value in list of contact/phone/email
# sets as the index all columns that must NOT be exploded first, then resets the index after.
headers = ['ProgramSpecialty','DataSourceURL','ProgramName',
           'ProgramURL', 'ProgramDetails', 'Available Per Cycle',
           'Alumni Publication Percentile', 'Alumni Clinical Trial Percentile', 'Founding Year']

program_df_name = program_df.explode('ProgramContactName').drop(labels=['ProgramContactPhone','ProgramContactEmail'], axis=1).reset_index()
program_df_phone = program_df.explode('ProgramContactPhone').drop(labels=headers+['ProgramContactName','ProgramContactEmail'], axis=1).reset_index()
program_df_email = program_df.explode('ProgramContactEmail').drop(labels=headers+['ProgramContactName','ProgramContactPhone'], axis=1).reset_index()

program_df_use = pd.concat([program_df_name,program_df_email,program_df_phone], axis=1)

program_df_use.to_csv('doximity_residency_scrape2023-08-22.csv', index=False, encoding='utf-8-sig')
