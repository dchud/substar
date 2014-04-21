#!/usr/bin/env python

"""
Process data collected using fetch.py into a single CSV file.
"""

import argparse
import csv
import json
import logging
import logging.config
import numpy
import os
from pprint import pprint

from settings import *

SIMPLE_FIELDS = ['id', 'owner', 'name', 
        'size', 'has_downloads', 'has_issues', 'has_wiki',
        'forks_count', 'network_count', 'stargazers_count', 
        'subscribers_count', 'watchers_count', 'open_issues_count',
        'fork']
DATE_FIELDS = ['created_at', 'updated_at', 'pushed_at']
COMPUTED_FIELDS = ['num_contributors', 'num_weeks', 'lines_added',
        'lines_added_per_week', 'lines_subtracted',
        'lines_subtracted_per_week', 'num_weeks_since_change', 'all_commits',
        'owner_commits', 'owner_commits_percentage', 'mean_commits_per_week',
        'std_commits_per_week']
ALL_FIELDS = SIMPLE_FIELDS + DATE_FIELDS + COMPUTED_FIELDS
print ','.join(ALL_FIELDS)

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('process')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datadir', default='data',
            help='data directory')
    args = parser.parse_args()
    langs = {}
    for fname in os.listdir(args.datadir):
        filename = '%s/%s' % (args.datadir, fname)
        logger.debug(filename)
        recs = json.load(open(filename))
        """STILL TO PROCESS:
        u'contributors', u'languages', u'participation',
        """
        for rec in recs:
            row = {}
            for field in SIMPLE_FIELDS:
                row[field] = rec.get(field, '')
            for field in DATE_FIELDS:
                row[field] = (rec.get(field, '') or '')[:10]
            for k, v in rec.get('languages', {}).items():
                try:
                    langs[k] += v
                except:
                    langs[k] = v

            # how many contributors?
            # FIXME: handle more extensive data from stats/contributors
            row['num_contributors'] = len(rec.get('contributors', []))

            # summarize code changes week over week
            added = subtracted = num_weeks = weeks_since = 0
            if rec.get('code_frequency', None):
                for w, a, s in rec['code_frequency']:
                    added += a
                    subtracted += s
                    if a or s:
                        weeks_since = 0
                    else:
                        weeks_since += 1
                num_weeks = len(rec['code_frequency'])
            row['num_weeks'] = num_weeks
            row['lines_added'] = added
            row['lines_added_per_week'] = added / num_weeks \
                    if num_weeks else 0
            row['lines_subtracted'] = subtracted
            row['lines_subtracted_per_week'] = subtracted / num_weeks \
                    if num_weeks else 0
            row['num_weeks_since_change'] = weeks_since

            # commits per week from all/owner over last 52 weeks
            all_commits = owner_commits = owner_commits_percentage = 0
            mean_commits_per_week = std_commits_per_week = 0 
            if rec.get('participation', []):
                all_commits = sum(rec['participation']['all'])
                owner_commits = sum(rec['participation']['owner'])
                owner_commits_percentage = 100.0 * owner_commits / all_commits \
                        if all_commits else 0
                mean_commits_per_week = all_commits / 52.0
                std_commits_per_week = numpy.std(rec['participation']['all'])
            row['all_commits'] = all_commits
            row['owner_commits'] = owner_commits
            row['owner_commits_percentage'] = owner_commits_percentage
            row['mean_commits_per_week'] = mean_commits_per_week
            row['std_commits_per_week'] = std_commits_per_week

            # Send it in, Jerome!
            print ','.join(str(row.get(f, '')) for f in ALL_FIELDS)
    #pprint(langs)
