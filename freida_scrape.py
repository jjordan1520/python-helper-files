import requests
from bs4 import BeautifulSoup
import re
import math
import time
import pandas as pd

# get initial page results
url = "https://freida.ama-assn.org/search/list?spec=42966"
page = requests.get(url)
html = page.content

# parse html with beautiful soup
soup_programs = BeautifulSoup(html, "html.parser")

# get max page number
max_result = soup_programs.find(class_="search-list__count ng-star-inserted").text
regex = re.compile(r".+(\d{3})")
max_result = int(regex.match(max_result).group(1))
max_page = math.ceil(max_result/25)

# empty list for links
links_programs = []

# get href for each program and add to links list
for link in soup_programs.find_all(class_="search-result-card__title"):
    links_programs.append((link["href"], link.text.strip()))

for page in range(2, (max_page + 1)):
    url_next = url + f"&page={page}"
    page_next = requests.get(url_next)
    html_next = page_next.content
    soup_programs_next = BeautifulSoup(html_next, "html.parser")
    for link in soup_programs_next.find_all(class_="search-result-card__title"):
        links_programs.append((link["href"], link.text.strip()))

# Work through the list of links to get each individual page html and parse that
base_url = "https://freida.ama-assn.org"
programs_content_list = []
for link in links_programs:
    page = requests.get(base_url + link[0])
    html = page.content
    programs_content_list.append((link[0], link[1], html))
    time.sleep(2)

souped_program_list = []
for program in programs_content_list:
    soup = BeautifulSoup(program[2], "html.parser")
    souped_program_list.append((program[0], program[1], soup))


def cfDecodeEmail(encodedString):
    """Decodes CloudFlare encoded email address"""
    r = int(encodedString[:2],16)
    email = ''.join([chr(int(encodedString[i:i+2], 16) ^ r) for i in range(2, len(encodedString), 2)])
    return email

# Empty list to hold dictionary for each program
program_dict_list = []

# Iterate through programs
for souped_html in souped_program_list:
    program_dict = {}
    # get name
    try:
        soup_names = souped_html[2].find_all(class_='contact-info__contacts__details')
        soup_contact_name = [name.contents[0] for name in soup_names]
    except AttributeError:
        soup_contact_name = ['']
    # get title
    try:
        soup_title = [title.contents[0].text for title in souped_html[2].find_all(class_="contact-info__contacts ng-star-inserted")]
    except AttributeError:
        soup_title = ['']
    # get email
    try:
        soup_email_data = [email['data-cfemail'] for email in souped_html[2].find_all(class_="__cf_email__")]
        soup_email = [cfDecodeEmail(email_data) for email_data in soup_email_data]
    except AttributeError:
        soup_email = ['']
    # Define each dictionary value and append to list
    program_dict['url'] = base_url + souped_html[0]
    program_dict['ProgramName'] = souped_html[1]
    program_dict['ContactName'] = soup_contact_name
    program_dict['ContactTitle'] = soup_title
    program_dict['Email'] = soup_email
    program_dict_list.append(program_dict)

# create data frame from list of dictionaries
program_df = pd.DataFrame(program_dict_list).fillna('')
# use series explode to create rows for each value in list of contact/phone/email
# sets as the index all columns that must NOT be exploded first, then resets the index after.
headers = ['url', 'ProgramName']
program_df_use = program_df.set_index(headers).apply(pd.Series.explode).reset_index()

program_df_use.to_csv('freida_ortho2.csv', index=False, encoding='utf-8-sig')


