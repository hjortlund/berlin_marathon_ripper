from queue import Queue
from threading import Thread, Lock
from requests import get
import json
import pandas as pd
import logging
from datetime import datetime


logging.basicConfig(filename='log/improved_{dt}.log'.format(dt=datetime.utcnow().strftime('%Y%m%d_%H%M%S')), level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())


df = pd.DataFrame()
lock = Lock()
queue = Queue()


def process_pages():
    global df, queue, lock

    column_mapping = {0: 'id',
                      1: 'place',
                      2: 'bib',
                      3: 'last_name',
                      4: 'first_name',
                      5: 'team',
                      6: 'nation',
                      7: 'yob',
                      8: 'sex',
                      9: 'age_class',
                      10: 'age_class_place',
                      11: 'net_time',
                      12: 'clock_time'}

    while True:
        url = queue.get()

        logging.debug('Downloading data from URL: {}'.format(url))

        res = get(url)
        res_json = json.loads(res.text)

        tmp_df = pd.DataFrame(columns=[key for key in column_mapping.values()])

        for row in res_json['rows']:
            row = row['cell']

            temp_dict = dict()
            for index, column in column_mapping.items():
                temp_dict[column] = row[index]

            for timing in ['net_time', 'clock_time']:
                h, m, s = temp_dict[timing].split(':')
                seconds = (int(h) * 60 * 60) + (int(m) * 60) + int(s)
                temp_dict[timing] = seconds

            tmp_df = tmp_df.append(temp_dict, ignore_index=True)

        lock.acquire()
        try:
            df = pd.concat([df, tmp_df], ignore_index=True)

        finally:
            lock.release()

        queue.task_done()


def numbers(query):
    res = get(query)
    res_json = json.loads(res.text)
    return int(res_json['total']), int(res_json['records'])


# Todo: Implement a nice little click cmd interface
def main(year=2017):
    threads = 10

    query_template = 'http://www.bmw-berlin-marathon.com/files/addons/scc_events_data/ajax.results.php?ci=MAL&t=BM_{year}&page={page}'
    pages, participants = numbers(query_template.format(year=year, page=1))

    logging.info('Pages: {}'.format(pages))
    logging.info('Participants: {}'.format(participants))

    for i in range(threads):
        logging.info('Launching Thread: {}'.format(i))
        t = Thread(target=process_pages)
        t.daemon = True
        t.start()

    for i in range(pages):
        queue.put(query_template.format(year=year, page=i))
        i += 1

    queue.join()
    df.to_csv('./data/{year}.csv'.format(year=year))


if __name__ == '__main__':
    main()
