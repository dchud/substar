#!/usr/bin/env python

"""
Process data collected using fetch.py into a single CSV file.
"""

import argparse
import json
import logging
import logging.config
import numpy
import os
from pprint import pprint

SIMPLE_FIELDS = ['id', 'owner', 'name', 'size', 'forks_count', 'network_count',
        'stargazers_count', 'open_issues_count',
        ]
BOOLEAN_FIELDS = ['has_downloads', 'has_issues', 'has_wiki', 'fork']
DATE_FIELDS = ['created_at', 'pushed_at']
COMPUTED_FIELDS = ['star10', 'num_contributors', 'num_weeks', 'lines_added',
        'lines_added_per_week', 'lines_subtracted',
        'lines_subtracted_per_week', 'num_weeks_since_change', 'all_commits',
        'owner_commits', 'owner_commits_percentage', 'mean_commits_per_week',
        'std_commits_per_week', 'lang0_prop', 'lang0', 'lang1', 'lang2',
        ]
ALL_FIELDS = SIMPLE_FIELDS + BOOLEAN_FIELDS + DATE_FIELDS + COMPUTED_FIELDS

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('process')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datadir', default='data',
            help='data directory')
    args = parser.parse_args()

    print '\t'.join(ALL_FIELDS)
    langs = {}
    for fname in os.listdir(args.datadir):
        filename = '%s/%s' % (args.datadir, fname)
        logger.debug(filename)
        recs = json.load(open(filename))
        for rec in recs:
            row = {}
            # NOTE: basic metadata about repositories overall
            for field in SIMPLE_FIELDS:
                row[field] = rec.get(field, '')
            for field in BOOLEAN_FIELDS:
                row[field] = '1' if rec.get(field, False) else '0'
            for field in DATE_FIELDS:
                value = (rec.get(field, '') or '')[:10]
                if value:
                    yyyy, mm, dd = value.split('-')
                    row[field] = '%s/%s/%s' % (mm, dd, yyyy)
                else:
                    row[field] = ''
            for k, v in rec.get('languages', {}).items():
                try:
                    langs[k] += v
                except:
                    langs[k] = v

            # are there at least 10 stargazers?
            # NOTE: use this for stratified sampling
            row['star10'] = True if int(rec['stargazers_count']) >= 10 \
                else False

            # NOTE: derived data about overall/recent development activity
            # how many contributors?
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

            # sort languages by usage
            langs_all_sorted = sorted([(int(v), k) for k, v
                in rec.get('languages', {}).items()], reverse=True)
            langs_all = [lang for size, lang in langs_all_sorted]
            # limit to the top 2
            for i in range(len(langs_all[:2])):
                row['lang%s' % i] = langs_all[i]

            # how much of the proportion of the code is the top language?
            try:
                total_size = sum([size for size, lang in langs_all_sorted])
                lang0_size = int(langs_all_sorted[0][0])
                # use a float to get the percentages, otherwise rounded to int
                row['lang0_prop'] = float(lang0_size) / total_size
            except:
                row['lang0_prop'] = 0

            # Render as a CSV line with tab separator
            print '\t'.join(str(row.get(f, '')) for f in ALL_FIELDS)
