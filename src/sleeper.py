import pandas as pd

from src.utils import api_get_request, format_response

BASE_URL = 'https://api.sleeper.app/v1'


class SleeperSeason:
    def __init__(self, season_id, league_name, base_url=BASE_URL):
        """
        Initialize the SleeperSeason class
        :param season_id: The most recent league (a.k.a. season) ID (Sleeper uses a new league ID for each season).
                          The league ID can be found at the end of the league's website URL.
                          A "league" in Sleeper's nomenclature is actually a single season.
        :param league_name: The name of the league to which season_id belongs
        :param base_url: The base URL of the Sleeper API
        """
        self.season_id = season_id
        self.league_name = league_name
        self.platform = 'sleeper'
        self.year = None
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
        """
        Request a season from the Sleeper API
        :param key_map: A dictionary representing the values to be parsed from the API response
        :return: A DataFrame representing the season
        """
        url = f'{self.base_url}/league/{self.season_id}'
        response = api_get_request(url)
        self.year = int(response.get('season'))
        self.previous_season_id = response.get('previous_league_id')
        self.draft_id = response.get('draft_id')
        settings = response.get('settings', dict())
        self.start_week = settings.get('start_week')
        self.playoff_start_week = settings.get('playoff_week_start')
        if key_map:
            response = format_response(response, key_map)
        self.season = pd.DataFrame([response])
        self.season['year'] = self.season['year'].astype(int)
        self.season['league_name'] = self.league_name
        return self.season

    def get_draft_picks(self, key_map=None):
        """
        Request the full list of draft picks from a particular draft from the Sleeper API
        :param key_map: A dictionary representing the values to be parsed from the API response
        :return: A DataFrame representing the draft picks
        """
        url = f'{self.base_url}/draft/{self.draft_id}/picks'
        response = api_get_request(url)
        if key_map:
            response = format_response(response, key_map)
        draft_picks = pd.DataFrame(response)
        draft_picks['season_id'] = self.season_id
        self.draft_picks = draft_picks
        return self.draft_picks

    def get_rosters(self, key_map=None):
        """
        Request the rosters for this season from the Sleeper API
        :param key_map: A dictionary representing the values to be parsed from the API response
        :return: A DataFrame representing the rosters
        """
        url = f'{self.base_url}/league/{self.season_id}/rosters'
        response = api_get_request(url)
        if key_map:
            response = format_response(response, key_map)
        rosters = pd.DataFrame(response)
        rosters['season_id'] = self.season_id
        self.rosters = rosters
        return self.rosters

    def get_winners_bracket(self, key_map=None):
        """
        Request the playoff winner's bracket for this season from the Sleeper API
        :param key_map: A dictionary representing the values to be parsed from the API response
        :return: A DataFrame representing the winner's bracket
        """
        url = f'{self.base_url}/league/{self.season_id}/winners_bracket'
        response = api_get_request(url)
        self.playoff_rounds_count = max([matchup['r'] for matchup in response])
        if key_map:
            response = format_response(response, key_map)
        winners_bracket = pd.DataFrame(response)
        winners_bracket['season_id'] = self.season_id
        self.winners_bracket = winners_bracket
        return self.winners_bracket

    def get_matchups(self, key_map=None):
        """
        Request the matchups for each week of this season from the Sleeper API
        :param key_map: A dictionary representing the values to be parsed from the API responses
        :return: A DataFrame representing all the matchups from this season
        """
        all_matchups = list()
        for week in range(self.start_week, self.playoff_start_week + self.playoff_rounds_count):
            url = f'{self.base_url}/league/{self.season_id}/matchups/{week}'
            response = api_get_request(url)
            if key_map:
                response = format_response(response, key_map)
            week_matchups = pd.DataFrame(response)
            week_matchups['season_id'] = self.season_id
            week_matchups['week'] = week
            all_matchups.append(pd.DataFrame(week_matchups))
        self.matchups = pd.concat(all_matchups, ignore_index=True)
        return self.matchups

    def get_users(self, key_map=None):
        """
        Request the users for this season from the Sleeper API
        :param key_map: A dictionary representing the values to be parsed from the API response
        :return: A DataFrame representing the users
        """
        url = f'{self.base_url}/league/{self.season_id}/users'
        response = api_get_request(url)
        if key_map:
            response = format_response(response, key_map)
        users = pd.DataFrame(response)
        users['season_id'] = self.season_id
        self.users = users
        return self.users
