import pandas as pd

from src.utils import api_get_request

BASE_URL = 'https://api.sleeper.app/v1'


class Season:
    def __init__(self, season_id, base_url=BASE_URL):
        """
        Initialize the Sleeper class
        :param season_id: The most recent league (a.k.a. season) ID (Sleeper uses a new league ID for each season).
                          The league ID can be found at the end of the league's website URL.
                          A "league" in Sleeper's nomenclature is actually a single season.
        """
        self.season_id = season_id
        self.base_url = base_url
        self.previous_season_id = None
        self.draft_id = None
        self.start_week = None
        self.playoff_start_week = None
        self.season = None
        self.draft_picks = None
        self.rosters = None
        self.playoff_rounds_count = None
        self.winners_bracket = None
        self.matchups = None
        self.users = None

    def get_season(self, key_map=None):
        url = f'{self.base_url}/league/{self.season_id}'
        response = api_get_request(url)
        self.previous_season_id = response.get('previous_league_id')
        self.draft_id = response.get('draft_id')
        settings = response.get('settings', dict())
        self.start_week = settings.get('start_week')
        self.playoff_start_week = settings.get('playoff_week_start')
        if key_map:
            response = self._format_response(response, key_map)
        self.season = pd.DataFrame([response])
        return self.season

    def get_draft_picks(self, key_map=None):
        url = f'{self.base_url}/draft/{self.draft_id}/picks'
        response = api_get_request(url)
        if key_map:
            response = self._format_response(response, key_map)
        draft_picks = pd.DataFrame(response)
        draft_picks['season_id'] = self.season_id
        self.draft_picks = draft_picks
        return self.draft_picks

    def get_rosters(self, key_map=None):
        url = f'{self.base_url}/league/{self.season_id}/rosters'
        response = api_get_request(url)
        if key_map:
            response = self._format_response(response, key_map)
        self.rosters = pd.DataFrame(response)
        return self.rosters

    def get_winners_bracket(self, key_map=None):
        url = f'{self.base_url}/league/{self.season_id}/winners_bracket'
        response = api_get_request(url)
        self.playoff_rounds_count = max([matchup['r'] for matchup in response])
        if key_map:
            response = self._format_response(response, key_map)
        winners_bracket = pd.DataFrame(response)
        winners_bracket['season_id'] = self.season_id
        self.winners_bracket = winners_bracket
        return self.winners_bracket

    def get_matchups(self, key_map=None):
        all_matchups = list()
        for week in range(self.start_week, self.playoff_start_week + self.playoff_rounds_count):
            url = f'{self.base_url}/league/{self.season_id}/matchups/{week}'
            response = api_get_request(url)
            if key_map:
                response = self._format_response(response, key_map)
            week_matchups = pd.DataFrame(response)
            week_matchups['season_id'] = self.season_id
            all_matchups.append(pd.DataFrame(week_matchups))
        self.matchups = pd.concat(all_matchups, ignore_index=True)
        return self.matchups

    def get_users(self, key_map=None):
        url = f'{self.base_url}/league/{self.season_id}/users'
        response = api_get_request(url)
        if key_map:
            response = self._format_response(response, key_map)
        users = pd.DataFrame(response)
        users['season_id'] = self.season_id
        self.users = users
        return self.users

    def _format_response(self, response, key_map):
        if isinstance(response, dict):
            formatted_response = self._format_record(response, key_map)
        else:
            formatted_response = list()
            for record in response:
                formatted_record = self._format_record(record, key_map)
                formatted_response.append(formatted_record)
        return formatted_response

    def _format_record(self, record, key_map):
        formatted_record = dict()
        for k, v in key_map.items():
            if record.get(k):
                self._format_key(record, k, v, formatted_record)
        return formatted_record

    def _format_key(self, record, key, value, formatted_record):
        if isinstance((sub_record := record[key]), dict):
            for sub_key, sub_value in value.items():
                self._format_key(sub_record, sub_key, sub_value, formatted_record)
        else:
            formatted_record[value or key] = record.get(key)
