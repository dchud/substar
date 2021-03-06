#!/usr/bin/env python

"""
Load up data about a number of repositories from github using their
API.  Report on results.
"""

import argparse
import glob
import json
import logging
import logging.config
from math import ceil
import os
import re
import time

import requests

from settings import *

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('fetch')

HEADERS = {'Authorization': 'token %s' % TOKEN}


def wait_buffer(req):
    """by default, wait this long between requests to follow github's
    rate limits."""
    reset_seconds = int(req.headers['x-ratelimit-reset']) - ceil(time.time())
    remaining = float(req_full_data.headers['x-ratelimit-remaining'])
    # pad it a little, just to have a friendly cushion
    buffer = 1.1 * reset_seconds / remaining
    # whenever the timer gets down close to a reset, add extra cushion
    # note also, buffer should never be negative
    if buffer < 0.1:
        buffer = 0.5
    logger.debug('wait: %s' % buffer)
    time.sleep(buffer)


def repo_api_request(owner, name, func, count=0):
    """
    Retry-able api requests; handle 202 responses with 1+-second delay
    retries up to MAX_RETRIES times with linear backoff.  Ignore rate 
    limit; 1+ seconds should always be longer than wait_buffer().
    """
    logger.debug('func: %s' % func)
    r = requests.get('https://api.github.com/repos/%s/%s/%s' % (owner,
        name, func), headers=HEADERS)
    wait_buffer(r)
    if r.status_code == 200:
        return r.json()
    elif r.status_code == 202:
        count += 1
        logger.debug('202 Accepted (count %s)' % count)
        # linear backoff: always wait at least one extra second per retry
        time.sleep(1 * count)
        if count <= MAX_RETRIES:
            return repo_api_request(owner, name, func, count=count)
    logging.error(r)
    return None


def save_recs(recs, count):
    # write out the file to disk as an indented json file
    filename = 'data/recs-%s.json' % count
    fp = open(filename, 'wb')
    json.dump(recs, fp, indent=2)
    fp.close()
    logger.debug('SAVED: %s' % filename)
    return filename


def next_url(url):
    # bump the sinceid 9900 to cycle through all repos over time
    # from oldest to most recent
    base, equalsign, sinceid = url.partition('=') 
    bumped_sinceid = int(sinceid) + 9900
    return '%s=%s' % (base, bumped_sinceid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='fetch github repo data')
    parser.add_argument('-a', '--append', action='store_true',
            default=False, help='pick up where we left off')
    args = parser.parse_args()

    if args.append:
        logger.debug('appending')
        files = glob.glob('data/*')
        oldest_file = max(files, key=os.path.getctime)
        logger.debug('oldest_file: %s' % oldest_file)
        old_data = json.load(open(oldest_file))
        oldest_rec_id = int(old_data[-1]['id'])
        logger.debug('oldest_rec_id: %s' % oldest_rec_id)
        bumped_rec_id = oldest_rec_id + 9900
        req_repos = requests.get(
                'https://api.github.com/repositories?since=%s' % bumped_rec_id)
        oldest_filename = oldest_file.split('/')[-1]
        oldest_count_id = re.findall(r'\d+', oldest_filename)[-1]
        count = int(oldest_count_id)
    else:
        # get a list of repos, starting from 0
        req_repos = requests.get('https://api.github.com/repositories',
            headers=HEADERS)
        count = 1
    recs = []
    next_repos_url = next_url(req_repos.links['next']['url'])
    repos = req_repos.json()
    while count <= FETCH_LIMIT:
        logger.debug('count: %s' % count)
        for repo in repos:
            owner = repo['owner']['login']
            name = repo['name']
            logger.debug('REPO: %s/%s' % (owner, name))

            # get full data
            # /repos/:owner/:repo
            req_full_data = requests.get(
                    'https://api.github.com/repos/%s/%s' % (owner, name),
                     headers=HEADERS)
            full_data = req_full_data.json()
            rec = {'owner': owner, 'name': name}
            for key in ['id', 'full_name', 'url', 'homepage', 'git_url',
                    'stargazers_count', 'watchers_count', 'subscribers_count',
                    'forks_count', 'size', 'fork', 'open_issues_count',
                    'has_issues', 'has_wiki', 'has_downloads', 'pushed_at',
                    'created_at', 'updated_at', 'network_count']:
                rec[key] = full_data.get(key, '')

            # NOTE: if there's never been a push, then forget it, jump
            #       to the next repo
            if rec['pushed_at'] is None:
                logger.debug('EMPTY, move on')
                continue

            parent_keys = ['id', 'fork', 'forks_count', 'stargazers_count',
                    'watchers_count', 'open_issues_count']
            wait_buffer(req_full_data)

            # get contributors
            # /repos/:owner/:repo/[stats/]contributors
            # note: user stats/contributors because plain contributors 
            #       paginates, even though it's a lot more data and can 202
            logger.debug('func: contributors')
            r = requests.get('https://api.github.com/repos/%s/%s/contributors' %
                    (owner, name), headers=HEADERS)
            wait_buffer(r)
            # secondary check for uninteresting repo
            if r.status_code == 204:
                logger.debug('204, move on')
                continue
            contributors = r.json()
            while r.links.has_key('next'):
                logger.debug('func: contributors')
                r = requests.get(r.links['next']['url'], headers=HEADERS)
                contributors.append(r.json())
                wait_buffer(r)
            rec['contributors'] = contributors

            # get participation
            # /repos/:owner/:repo/stats/participation
            participation = repo_api_request(owner, name, 'stats/participation')
            if participation:
                rec['participation'] = participation

            # get languages
            # /repos/:owner/:repo/languages
            languages = repo_api_request(owner, name, 'languages')
            if languages:
                rec['languages'] = languages

            # get code frequency
            # /repos/:owner/:repo/stats/code_frequency
            code_frequency = repo_api_request(owner, name,
                    'stats/code_frequency')
            if code_frequency:
                rec['code_frequency'] = code_frequency

            # get teams
            # /repos/:owner/:repo/teams
            # NOTE: url pattern 404s across repos
            #teams = repo_api_request(owner, name, 'teams')
            #if teams:
            #    rec['teams'] = teams
            
            # get hierarchy 
            if full_data['fork']:
                if full_data['parent']['id'] != full_data['id']:
                    rec['parent'] = full_data['parent']
                    for key in parent_keys:
                        rec['parent_%s' % key] = rec['parent'].get(key, '')
                if full_data['source']['id'] != full_data['parent']['id']:
                    rec['source'] = full_data['source']
                    for key in parent_keys:
                        rec['source_%s' % key] = rec['source'].get(key, '')

            recs.append(rec)
            if len(recs) == 100:
                save_recs(recs, count)
                recs = []

            count += 1
            logger.debug('count: %s' % count)
            if count == FETCH_LIMIT:
                break
        logger.debug('FETCH: %s' % next_repos_url)
        req_repos = requests.get(next_repos_url)
        next_repos_url = next_url(req_repos.links['next']['url'])
        repos = req_repos.json()

if recs:
    save_recs(recs, count)
