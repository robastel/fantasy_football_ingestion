import numpy as np
from google.cloud import bigquery, storage
import dask.dataframe as dd

from src.utils import get_logger, parse_args, parse_yaml, get_data_types
from src.sleeper import SleeperSeason
from src.espn import ESPNSeason

league_config = parse_yaml('config.yaml')


class Main:
    def __init__(self, config):
        # Configurations
        self.config = config
        self.tables_config = self.config.get('tables', dict())
        # Sleeper configurations
        self.sleeper_base_url = self.config.get('sleeper_base_url')
        # ESPN configurations
        self.espn_start_year = self.config.get('espn_start_year', dict())
        self.espn_end_year = self.config.get('espn_end_year', dict())
        # Command line arguments
        self.config = config
        self.args = parse_args(self.config['args'])
        self.league_name = self.args['league_name']
        self.sleeper_season_id = self.args['sleeper_season_id']
        self.gcs_bucket = self.args['gcs_bucket']
        self.gbq_project = self.args['gbq_project']
        self.gbq_dataset = self.args['gbq_dataset']
        self.espn_league_id = self.args.get('espn_league_id')
        self.espn_s2 = self.args.get('espn_s2')
        self.espn_swid = self.args.get('espn_swid')
        self.logger = get_logger('Fantasy Football Stats', level=self.args['log_level'])
        # Create GCP clients
        self.gcs_client = storage.Client()
        self.gbq_client = bigquery.Client()

    def __call__(self):
        season = SleeperSeason(self.sleeper_season_id, self.league_name, base_url=self.sleeper_base_url)
        is_season_loaded = False
        while season.season_id and not is_season_loaded:
            self.load_season(season)
            if self.espn_start_year < season.year <= self.espn_end_year + 1:
                season = ESPNSeason(
                    self.espn_league_id, self.league_name, self.espn_s2, self.espn_swid, season.year - 1
                )
            else:
                season = SleeperSeason(
                    getattr(season, 'previous_season_id', None), self.league_name, base_url=self.sleeper_base_url
                )
            is_season_loaded = self.check_season_loaded(season)

        gbq_client = bigquery.Client()
        for platform, tables in self.tables_config.items():
            for table, table_config in tables.items():
                table_name = f'{platform}_{table}'
                uri = f'gs://{self.gcs_bucket}/{table_name}/{self.league_name}/*.parquet'
                table_id = f'{self.gbq_project}.{self.gbq_dataset}.{table_name}'
                data_types = get_data_types(table_config['key_map'])
                schema = [bigquery.SchemaField(col_name, data_type) for col_name, data_type in data_types.items()]
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    source_format=bigquery.SourceFormat.PARQUET
                )
                load_job = gbq_client.load_table_from_uri(uri, table_id, job_config=job_config)
                load_job.result()  # Waits for the job to complete.
                self.logger.info(f'Loaded data from location "{uri}" to table "{table_id}"')

    def check_season_loaded(self, season_obj):
        season_prefix = f'{season_obj.platform}_seasons/{self.league_name}/{season_obj.season_id}'
        season_blob_iterator = self.gcs_client.list_blobs(self.gcs_bucket, prefix=season_prefix)
        season_blobs_count = sum([page.num_items for page in season_blob_iterator.pages])
        is_loaded = True if season_blobs_count > 0 else False
        return is_loaded

    def load_season(self, season_obj):
        self.logger.info(f'Started extracting season "{season_obj.season_id}" from {season_obj.platform} to GCS')
        platform_tables_config = self.tables_config[season_obj.platform]
        for table, table_config in platform_tables_config.items():
            method_name = table_config['method']
            method = season_obj.__getattribute__(method_name)
            df = method(key_map=table_config.get('key_map'))
            df = df.replace('', np.nan)
            dask_df = dd.from_pandas(df, npartitions=1)
            gcs_path = f'gs://{self.gcs_bucket}/{season_obj.platform}_{table}/{self.league_name}/{season_obj.season_id}'
            schema = get_data_types(table_config.get('key_map'))
            dd.to_parquet(dask_df, path=gcs_path, write_index=False, overwrite=True, schema=schema, engine='pyarrow')
            self.logger.info(f'Extracted to {gcs_path}')
        self.logger.info(f'Finished extracting season "{season_obj.season_id}" from {season_obj.platform} to GCS\n')


if __name__ == '__main__':
    m = Main(league_config)
    m()
