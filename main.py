import pandas as pd
from google.oauth2 import service_account

from src.utils import get_logger, parse_args, parse_yaml
from src.sleeper import SleeperSeason
from src.espn import ESPNSeason

# Define the command line arguments configuration
args_config = [
    {
        'definition': 'sleeper_season_id',
        'params': {
            'help': 'The league ID of the most recent Sleeper season'
        }
    },
    {
        'definition': 'gcp_credentials',
        'params': {
            'help': 'The location of the Google Cloud Computing (GCP) credentials file'
        }
    },
    {
        'definition': 'gbq_project',
        'params': {
            'help': 'The Google BigQuery Project ID'
        }
    },
    {
        'definition': 'gbq_dataset',
        'params': {
            'help': 'The Google BigQuery dataset to output tables to'
        }
    },
    {
        'definition': ['-e', '--espn-league-id'],
        'params': {
            'help': 'The ESPN league ID'
        }
    },
    {
        'definition': ['-s', '--espn-s2'],
        'params': {
            'help': 'Your espn_s2 cookie'
        }
    },
    {
        'definition': ['-w', '--espn-swid'],
        'params': {
            'help': 'The ESPN swid cookie'
        }
    },
    {
        'definition': ['-l', '--log-level'],
        'params': {
            'default': 'INFO',
            'help': 'A python logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)'
        }
    }
]

# Get command line argument configurations
args = parse_args(args_config)
logger = get_logger('Fantasy Football Stats', level=args['log_level'])
espn_league_id = args['espn_league_id']
espn_s2 = args['espn_s2']
espn_swid = args['espn_swid']
sleeper_season_id = args['sleeper_season_id']
gbq_creds = service_account.Credentials.from_service_account_file(args['gcp_credentials'])
gbq_project = args['gbq_project']
gbq_dataset = args['gbq_dataset']

# Get yaml file configurations
config = parse_yaml('config.yaml')
# ESPN configurations
espn_config = config['espn']
espn_start_year = espn_config['years']['start']
espn_end_year = espn_config['years']['end']
espn_tables_config = espn_config['tables']
# Sleeper configurations
sleeper_config = config['sleeper']
sleeper_base_url = sleeper_config['base_url']
sleeper_tables_config = sleeper_config['tables']

# Create a dictionary to store all the DataFrames that will later be uploaded to BigQuery
dfs = {
    **{t: list() for t in espn_tables_config},
    **{t: list() for t in sleeper_tables_config}
}


def build_season_dfs(season_obj, tables_config, dfs_dict):
    """
    Helper function to build and collect all the dataframes necessary for each season
    :param season_obj: A SleeperSeason or ESPNSeason object
    :param tables_config: A configuration dictionary containing a dictionary of tables for which to build DataFrames.
                          Each key is a table name and has a nested dictionary as its value.
                          Each nested dictionary contains a 'method' key to denote which method of season_obj to run.
                          Each nested dictionary may also contains a 'key_map' key that is used to filter, flatten, and
                          rename the the values in each record from the API response.  See src.utils.format_response()
                          for more details.
    :param dfs_dict: A dictionary where the keys are table names and the values are lists of DataFrames.
                     Each list of DataFrames will later be concatenated before upload to the corresponding table name.
    :return: None
    """
    for table_name, table_config in tables_config.items():
        method_name = table_config['method']
        method = season_obj.__getattribute__(method_name)
        dfs_dict[table_name].append(method(key_map=table_config.get('key_map')))


if __name__ == '__main__':
    # Get Sleeper seasons
    season = SleeperSeason(sleeper_season_id, base_url=sleeper_base_url)
    while season.season_id:
        build_season_dfs(season, sleeper_tables_config, dfs)
        logger.info(f'Got the {season.season["year"].iloc[0]} season from Sleeper')
        season = SleeperSeason(season.previous_season_id)
    # Get ESPN seasons
    for year in range(espn_end_year, espn_start_year-1, -1):
        season = ESPNSeason(espn_league_id, espn_s2, espn_swid, year)
        build_season_dfs(season, espn_tables_config, dfs)
        logger.info(f'Got the {year} season from ESPN')
    # Upload to BigQuery
    for table, data in dfs.items():
        df = pd.concat(data, ignore_index=True)
        df.to_gbq(f'{gbq_dataset}.{table}', project_id=gbq_project, if_exists='replace', credentials=gbq_creds)
        logger.info(f'Created or replaced table "{gbq_dataset}.{table}" in BigQuery')
