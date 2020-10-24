import pandas as pd
from google.oauth2 import service_account

from src.utils import get_logger, parse_args
from src.sleeper import Season

logger = get_logger('Fantasy Football Stats')
args_config = [
    {
        'definition': ['-s', '--sleeper'],
        'params': {
            'help': 'Most recent Sleeper season league ID'
        }
    },
    {
        'definition': ['-c', '--creds'],
        'params': {
            'help': 'Location of the Google BigQuery credentials file'
        }
    },
    {
        'definition': ['-p', '--project'],
        'params': {
            'help': 'The Google BigQuery Project ID'
        }
    },
    {
        'definition': ['-d', '--dataset'],
        'params': {
            'help': 'The Google BigQuery dataset to output tables to'
        }
    }
]
args = parse_args(args_config)
sleeper_season_id = args['sleeper']
creds = service_account.Credentials.from_service_account_file(args['creds'])
project = args['project']
dataset = args['dataset']

if __name__ == '__main__':
    seasons = list()
    s = Season(args['sleeper'])
    while s.season_id:
        s()
        seasons.append(s)
        s = Season(s.season['previous_league_id'])

    tables = {
        'draft_picks': list(),
        'rosters': list(),
        # 'matchups_h2h': list(),
        # 'matchups_median: list(),
    }
    for season in seasons:
        tables['draft_picks'].append(season.draft.picks)
        tables['rosters'].append(season.rosters)
        # tables['matchups_h2h'].append(season.matchups_h2h)
        # tables['matchups_median'].append(season.matchups_median)
    for table_name, data in tables.items():
        df = pd.concat(data)
        df.to_gbq(f'{dataset}.{table_name}', project_id=project, if_exists='replace', credentials=creds)

    # TODO: create seasons table with year and an all-time users table with most recent season information
