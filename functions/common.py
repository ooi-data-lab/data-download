#! /usr/bin/env python
import os
import requests
import re
import time


def check_request_status(thredds_url):
    check_complete = thredds_url.replace('/catalog/', '/fileServer/')
    check_complete = check_complete.replace('/catalog.html', '/status.txt')
    session = requests.session()
    r = session.get(check_complete)
    while r.status_code != requests.codes.ok:
        print('Data request is still fulfilling. Trying again in 1 minute.')
        time.sleep(60)
        r = session.get(check_complete)
    print('Data request has fulfilled.')


def create_dir(new_dir):
    # Check if dir exists.. if it doesn't... create it.
    if not os.path.isdir(new_dir):
        try:
            os.makedirs(new_dir)
        except OSError:
            if os.path.exists(new_dir):
                pass
            else:
                raise
