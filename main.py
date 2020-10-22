import logging
import sys
import argparse

from src.sleeper import Season

logger = logging.getLogger('Fantasy Football Stats')
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('{name} [{levelname}]:: {message}', style='{')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel('INFO')

ap = argparse.ArgumentParser()
ap.add_argument('-s', '--sleeper', help='Most recent Sleeper season league ID')
args = vars(ap.parse_args())

if __name__ == '__main__':
    seasons = list()
    s = Season(args['sleeper'])
    while s.season_id:
        s()
        seasons.append(s)
        s = Season(s.season['previous_league_id'])

    for season in seasons:
        logger.info(f"Season: {season.season['season']}, Last Completed Week: {season.last_completed_week}")
