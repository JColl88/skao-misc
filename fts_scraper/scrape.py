import csv
import time

from bs4 import BeautifulSoup, Comment
from selenium import webdriver

driver = webdriver.Firefox()
entries = []
for page in range(1,121):
    print(page)
    #driver = webdriver.Firefox()
    driver.get('https://fts3-pilot.cern.ch:8449/fts3/ftsmon/#/optimizer/detailed?source=davs:%2F%2Fsrcdev.skatelescope.org&destination=davs:%2F%2Fspsrc14.iaa.csic.es&time_window=700&page={}'.format(page))

    time.sleep(2)

    html = driver.page_source
    soup = BeautifulSoup(html, features='xml')

    tr_tags = soup.find_all('tr', {'data-ng-repeat': "o in optimizer.evolution.items"})
    for tr_tag in tr_tags:
        entry = {}
        td_tags = tr_tag.find_all('td', {'class': 'ng-binding'})
        entry['timestamp'] = td_tags[0].text
        entry['explanation'] = td_tags[1].text

        td_tags = tag.find_all('td', {'class': 'numeric ng-binding'})
        entry['decision'] = td_tags[0].text
        entry['running'] = td_tags[1].text
        entry['queue'] = td_tags[2].text
        entry['success_rate'] = td_tags[3].text
        entry['throughput'] = td_tags[4].text
        entry['ema'] = td_tags[5].text
        entry['diff'] = td_tags[6].text

        entries.append(entry)
driver.quit()

with open('out.csv', 'w') as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow(entries[0].keys())
    for row in entries:
        csv_writer.writerow(row.values())
