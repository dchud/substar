#!/usr/bin/env python

"""
Load up data about a number of repositories from github using their
API.  Report on results.
"""

import json
import logging
from math import ceil
import time

import requests

from settings import *

logging.basicConfig(level=logging.DEBUG)
logging.debug('TOKEN: ' + TOKEN)
logging.debug('MAX_RETRIES: %s' % MAX_RETRIES)
logging.debug('FETCH_LIMIT: %s' % FETCH_LIMIT)

HEADERS = {'Authorization': 'token %s' % TOKEN}


def wait_buffer(req):
    """by default, wait this long between requests to follow github's
    rate limits."""
    reset_seconds = int(req.headers['x-ratelimit-reset']) - ceil(time.time())
    logging.debug('reset_seconds: %s' % reset_seconds)
    remaining = float(req_full_data.headers['x-ratelimit-remaining'])
    logging.debug('remaining: %s' % remaining)
    buffer = reset_seconds / remaining
    logging.debug('wait: %s' % buffer)
    time.sleep(buffer)


def repo_api_request(owner, name, func, count=0):
    logging.debug('request: %s' % func)
    r = requests.get('https://api.github.com/repos/%s/%s/%s' % (owner,
        name, func), headers=HEADERS)
    wait_buffer(r)
    if r.status_code == 200:
        return r.json()
    elif r.status_code == 202:
        count += 1
        logging.debug('202 Accepted (count %s)' % count)
        if count <= MAX_RETRIES:
            return repo_api_request(func, owner, name, count=count)
    logging.error(r)
    return None


def save_recs(recs, count):
    filename = 'data/recs-%s.json' % count
    fp = open(filename, 'wb')
    json.dump(recs, fp, indent=2)
    fp.close()
    logging.debug('SAVED: %s' % filename)
    return filename


if __name__ == '__main__':
    count = 0
    recs = []
    # get a list of repos
    req_repos = requests.get('https://api.github.com/repositories',
        headers=HEADERS)
    repos = req_repos.json()
    for repo in repos:
        count += 1
        if count > FETCH_LIMIT:
            break
        # get full data
        # /repos/:owner/:repo
        owner = repo['owner']['login']
        logging.debug('repo-owner %s' % owner)
        name = repo['name']
        logging.debug('repo-name %s' % name)
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
        parent_keys = ['id', 'fork', 'forks_count', 'stargazers_count',
                'watchers_count', 'open_issues_count']
        wait_buffer(req_full_data)

        # get contributors
        # /repos/:owner/:repo/[stats/]contributors
        contributors = repo_api_request(owner, name, 'contributors')
        if contributors:
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

if recs:
    save_recs(recs, count)
