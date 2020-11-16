import pandas as pd
from google.cloud import bigquery

from src.utils import get_logger, parse_args, parse_yaml, get_data_types
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
espn_league_id = args.get('espn_league_id')
espn_s2 = args.get('espn_s2')
espn_swid = args.get('espn_swid')
sleeper_season_id = args['sleeper_season_id']
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


def build_season_dfs(season_obj, tables_config, output_dict, dfs_key):
    """
    Helper function to build and collect all the dataframes necessary for each season
    :param season_obj: A SleeperSeason or ESPNSeason object
    :param tables_config: A configuration dictionary containing a dictionary of tables for which to build DataFrames.
                          Each key is a table name and has a nested dictionary as its value.
                          Each nested dictionary contains a 'method' key to denote which method of season_obj to run.
                          Each nested dictionary may also contains a 'key_map' key that is used to filter, flatten, and
                          rename the the values in each record from the API response.  See src.utils.format_response()
                          for more details.
    :param output_dict: A dictionary where the keys are table names and the values are nested dictionaries.
    :param dfs_key: The key inside each of the nested dictionaries from output_dict that contains a list of DataFrames.
                    Each list of DataFrames will later be concatenated before upload to the corresponding table name.
    :return: None
    """
    for table_name, table_config in tables_config.items():
        method_name = table_config['method']
        method = season_obj.__getattribute__(method_name)
        output_dict[table_name][dfs_key].append(method(key_map=table_config.get('key_map')))


# Create a dictionary to store all the DataFrames and data types that will later be uploaded to BigQuery
output = {
    **{t: {'dfs': list(), 'data_types': get_data_types(cfg['key_map'])} for t, cfg in espn_tables_config.items()},
    **{t: {'dfs': list(), 'data_types': get_data_types(cfg['key_map'])} for t, cfg in sleeper_tables_config.items()}
}

if __name__ == '__main__':
    # Get Sleeper seasons
    season = SleeperSeason(sleeper_season_id, base_url=sleeper_base_url)
    while season.season_id:
        build_season_dfs(season, sleeper_tables_config, output, 'dfs')
        logger.info(f'Got the {season.season["year"].iloc[0]} season from Sleeper')
        season = SleeperSeason(season.previous_season_id)
    # Get ESPN seasons
    if espn_league_id:
        for year in range(espn_end_year, espn_start_year-1, -1):
            season = ESPNSeason(espn_league_id, espn_s2, espn_swid, year)
            build_season_dfs(season, espn_tables_config, output, 'dfs')
            logger.info(f'Got the {year} season from ESPN')
    # Upload to BigQuery
    gbq_client = bigquery.Client()
    for table, data in output.items():
        df = pd.concat(data['dfs'], ignore_index=True)
        job_config = bigquery.LoadJobConfig(schema=data['data_types'])
        print(df.columns)
        print(data['data_types'])
        job = gbq_client.load_table_from_dataframe(df, f"{gbq_dataset}.{table}", job_config=job_config)
        job.result()
        logger.info(f'Created or replaced table "{gbq_dataset}.{table}" in BigQuery')
