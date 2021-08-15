
import re
import os
import sys
import datetime
import logging

import numpy as np

logging.basicConfig(format='%(asctime)s - %(message)s',
                    level=logging.INFO,
                    filename='domain_tags.log',
                    filemode='w')

import pandas as pd

from selenium import webdriver

from bs4 import BeautifulSoup

options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--incognito')
options.add_argument('--headless')
chromium_path = os.path.abspath(r"C:\Users\bpine\PycharmProjects\taganalyzer\needed\chromedriver.exe")
driver = webdriver.Chrome(chromium_path, options=options)

# Global variables
start_time = datetime.datetime.now()
domain_tag_df = pd.DataFrame()
domain_tag_df['saved'] = True
already_saved = pd.DataFrame()
domain_tags_queued_df = pd.DataFrame()
to_save_in_master_df = pd.DataFrame()

#Functions

def re_search(regex_code, search):
    re_domain = re.compile(regex_code)
    return re_domain.findall(search)

def find_domain(search):
    regex_code = r'\b(?<=//)((?:[\w-]+\.){1,}\w+)\b'
    return re_search(regex_code, search)

def loop_data():
    global domain_tags_queued_df, sites_to_update, tag_df, domain_tags_queued_df
    # Iterate through publisher domains3
    sites_list = list(sites_to_update)
    sites_list.sort()
    i = 0
    for i, site in enumerate(list(sites_list)):
        print(site)
        logging.info(f'Publishers site: {site}')
        # Load home page of a given domain
        try:
            page = load_page_soup(site)
            # Find all scripts in header and main page and the domains of the script 'src'
            script_list = page.find_all('script')
            unique_domains = find_src_domains(script_list)

            if len(unique_domains) > 0:
               tag_df = pd.DataFrame.from_dict({'tag_domain': unique_domains})
               tag_df['site'] = site
               logging.info(f'Unique tag src found: {len(unique_domains)}')
               tag_df['status'] = 'OK'
            else:
               tag_df = pd.DataFrame(columns=['site'])
               tag_df.loc[0] = site
               tag_df['tag_domain'] = None
               tag_df['status'] = 'No script scr tags on site home page'
               logging.warning('No script scr tags on site home page')
        except:
            tag_df = pd.DataFrame(columns=['site'])
            tag_df.loc[0] = site
            tag_df['status'] = 'Publisher domain not accessible'
            logging.warning('Publisher domain not accessible')

        tag_df['scrape_date'] = datetime.datetime.now().date()
        domain_tags_queued_df = domain_tags_queued_df.append(tag_df)
        logging.info(tag_df)
    # Save queued results from 10 (or more) unsaved publisher site tags
    # Results are saved once every ten publishers
        if (i+1) % 15 == 0:
            logging.info(f'Saving queued data')
            logging.info(domain_tags_queued_df)
            save_queued_data()
            elapsed_time_status(i+1, sites_to_update)
    return i

def elapsed_time_status(i, sites_to_update):

    current_time = datetime.datetime.now()
    elapsed_seconds = int((current_time - start_time).total_seconds())
    percent_complete = (i / len(sites_to_update))
    est_remaining_seconds = (elapsed_seconds / percent_complete)
    print(f'{i} publishers have been processed in {elapsed_seconds} seconds. Representing {percent_complete:.4f}% of target publishers.')
    print(f'Processing estimated to be complete in {int(est_remaining_seconds/60):0} minutes.')

def find_src_domains(script_list):
    all_domains=list()
    k='src'
    for script in script_list:
        if k in script.attrs:
            script_src = find_domain(script.attrs[k])
            if len(script_src) > 0:
                 for src in script_src:
                    all_domains.append(src)
    return list(set(all_domains))

def load_page_soup (domain_name):
    url = ''.join(['https://', domain_name])

    driver.get(url)
    page = driver.page_source
    return BeautifulSoup(page, "html.parser")

# noinspection PyBroadException
def load_pub_data():
    global pub_data_df, pub_sites,master_tag_data_df
    file_path = os.path.abspath(
        r'C:\Users\bpine\PycharmProjects\taganalyzer\needed\liveintent_Traffic_dump_2021-01-01_2021-07-31.csv')
    if os.path.exists(file_path):
        pub_data_df = pd.read_csv(file_path, infer_datetime_format=True)
        pub_data_df['Publisher Domain'] = pub_data_df['Publisher Domain'].astype(str)
        pub_sites = set(pub_data_df.loc[pub_data_df['Publisher Domain'] != 'nan']['Publisher Domain'].values)
    else:
        logging.critical('File with publisher domains is not found or unloadable from:')
        logging.critical(str(file_path))
        sys.exit()

def load_master_tag_data(last_update=3):
    # Load existing publisher domain tags data
    global master_tag_data_df, sites_to_update
    last_update_date = datetime.datetime.now() + datetime.timedelta(days=-last_update)
    file_path = os.path.abspath(
        r'C:\Users\bpine\PycharmProjects\taganalyzer\master_tag_data.csv')
    if os.path.exists(file_path):
        master_tag_data_df = pd.read_csv(file_path)
        master_tag_data_df.scrape_date = pd.to_datetime(master_tag_data_df.scrape_date)
        logging.info('Loaded master_tag_data.csv as master_tag_data_df')
        master_tag_data_df.site = master_tag_data_df.site.astype(str)
        master_updated_sites = set(master_tag_data_df
                                     .loc[((master_tag_data_df.scrape_date > last_update_date) &
                                          (master_tag_data_df.site != 'nan'))].site)
        logging.info('The following sites have been updated recently or are missing domains and will be excluded')
        logging.info(master_updated_sites)

    else:
        logging.critical('Master file of existing tags for each domain cannot be found')
        logging.critical('New file begin created')
        master_tag_data_df = pd.DataFrame()
        master_updated_sites = set()

    file_path = os.path.abspath(
        r'C:\Users\bpine\PycharmProjects\taganalyzer\to_save_in_master.csv')
    if os.path.exists(file_path):
        already_queued = pd.read_csv(file_path)
        logging.info(f'Loaded existing to_save_in_master.csv with {len(already_queued)} records')
        already_queued_sites = set(already_queued.site.unique())
    else:
        already_queued_sites = set()
        logging.info(f'No to_save_in_master.csv creating empty dataframe')

    logging.info(f'Loaded {len(pub_sites)} unique publisher sites')
    logging.info('The following sites have been updated in master_tag_data recently and will not be updated')
    logging.info(f'{master_updated_sites}')
    logging.info('The following sites have been previous queued to be saved and will not be updated')
    logging.info(f'{already_queued_sites}')

    master_sites_to_be_updated = pub_sites.difference(master_updated_sites)
    sites_to_update = master_sites_to_be_updated.difference(already_queued_sites)

    logging.info(f'{len(master_sites_to_be_updated)} : difference between {len(pub_sites)} and {len(master_updated_sites)}')
    logging.info(f'{len(sites_to_update)} : difference between {len(master_sites_to_be_updated)} and {len(already_queued_sites)}')


def save_queued_data():
    global to_save_in_master_df, domain_tags_queued_df
    to_save_in_master_df = to_save_in_master_df.append(domain_tags_queued_df)
    logging.info(f'Preparing to save {len(to_save_in_master_df)} queued results, {len(already_saved)} previously queue results already saved.')
    logging.info(f'Data being saved')
    logging.info(to_save_in_master_df)
    file_path = os.path.abspath(
        r'C:\Users\bpine\PycharmProjects\taganalyzer\to_save_in_master.csv')
    to_save_in_master_df.to_csv(file_path, index=False)
    domain_tags_queued_df = pd.DataFrame()

# Main Program
days_since_last_update = 3
load_pub_data()
load_master_tag_data(last_update=days_since_last_update)
logging.info(f'{len(pub_sites)} publisher sites loaded.')
logging.info(f'{len(sites_to_update)} sites will be analyzed.')
last_i = loop_data()
save_queued_data()
print(f'{len(master_tag_data_df)} records already saved. Adding {len(to_save_in_master_df)} records.')
if len(to_save_in_master_df) > 0:
    to_save_in_master_df.scrape_date = pd.to_datetime(to_save_in_master_df.scrape_date, infer_datetime_format=True)
    new_master_df = master_tag_data_df.append(to_save_in_master_df, ignore_index=True)
    # new_master_df = new_master_df.drop(columns='saved')
    # os.rename('master_tag_data.csv','master_tag_data_old.csv')
    logging.info(f'Write master_tag_data.csv file with {len(new_master_df)} records')
    file_path = os.path.abspath(
    r'C:\Users\bpine\PycharmProjects\taganalyzer\master_tag_data.csv')
    new_master_df.to_csv(file_path, index=False)
else:
    print('No records added -- no files saved or updated')

file_path = os.path.abspath(
    r'C:\Users\bpine\PycharmProjects\taganalyzer\to_save_in_master.csv')
if os.path.exists(file_path):
    logging.info(f'Removing to_save_in_master.csv with {len(to_save_in_master_df)} records')
    os.remove(file_path)
