import pandas as pd
from google.oauth2 import service_account

from src.utils import get_logger, parse_args, parse_yaml
from src.sleeper import Season

logger = get_logger('Fantasy Football Stats')

args_config = [
    {
        'definition': ['-s', '--sleeper-season-id'],
        'params': {
            'help': 'The league ID of the most recent Sleeper season'
        }
    },
    {
        'definition': ['-c', '--credentials'],
        'params': {
            'help': 'The location of the Google BigQuery credentials file'
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
sleeper_season_id = args['sleeper_season_id']
gbq_credentials = service_account.Credentials.from_service_account_file(args['credentials'])
gbq_project = args['project']
gbq_dataset = args['dataset']

config = parse_yaml('config.yaml')
sleeper_config = config['sleeper']
sleeper_base_url = sleeper_config['base_url']
sleeper_tables_config = sleeper_config['tables']
sleeper_dfs = {t: list() for t in sleeper_tables_config}

if __name__ == '__main__':
    season = Season(sleeper_season_id, base_url=sleeper_base_url)
    while season.season_id:
        for table, table_config in sleeper_tables_config.items():
            method_name = table_config['method']
            method = season.__getattribute__(method_name)
            sleeper_dfs[table].append(method(key_map=table_config.get('key_map')))
        season = Season(season.previous_season_id)

    for table, data in sleeper_dfs.items():
        df = pd.concat(data)
        df.to_gbq(f'{gbq_dataset}.{table}', project_id=gbq_project, if_exists='replace', credentials=gbq_credentials)
