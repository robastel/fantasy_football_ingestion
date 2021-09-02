import pandas as pd

from espn_api.football import League

from src.utils import format_response


class ESPNSeason:
    def __init__(self, league_id, league_name, s2, swid, year):
        """
        Initialize the ESPNSeason class
        :param league_id: The ESPN league ID for which this season is a part of
        :param league_name: The name of the league to which this season belongs
        :param s2: Your ESPN s2 cookie (used for authentication)
        :param swid: Your ESPN swid cookie (used for authentication)
        :param year: The year of the season
        """
        self.league_id = league_id
        self.league_name = league_name
        self.platform = 'espn'
        self.s2 = s2
        self.swid = swid
        self.year = year
        self.season_id = f'{self.league_id}_{self.year}'
        self.response = None
        self.season = None
        self.start_week = None
        self.regular_season_weeks = None
        self.team_objs = None
        self.teams = None
        self.draft_picks = None
        self.player_map = None
        self.matchups = None

    def get_season(self, key_map=None):
        """
        Request a season from ESPN
        :param key_map: A dictionary representing the values to be parsed from the API response
        :return: A DataFrame representing the season
        """
        self.response = League(league_id=self.league_id, year=self.year, espn_s2=self.s2, swid=self.swid)
        self.response = vars(self.response)
        self.response['settings'] = vars(self.response['settings'])
        self.start_week = self.response.get('firstScoringPeriod', 1)
        self.regular_season_weeks = self.response['settings'].get('reg_season_count')
        self.team_objs = [vars(team) for team in self.response['teams']]
        self.draft_picks = [vars(pick) for pick in self.response['draft']]
        self.player_map = self.response['player_map']
        if key_map:
            self.response = format_response(self.response, key_map)
        self.season = pd.DataFrame([self.response])
        self.season['season_id'] = self.season_id
        self.season['league_name'] = self.league_name
        return self.season

    def get_draft_picks(self, key_map=None):
        """
        Parse the draft picks for the season
        :param key_map: A dictionary representing the values to be parsed from the raw draft picks data
        :return: A DataFrame representing the draft picks
        """
        for pick in self.draft_picks:
            pick['team'] = vars(pick['team'])
        if key_map:
            self.draft_picks = format_response(self.draft_picks, key_map)
        self.draft_picks = pd.DataFrame(self.draft_picks)
        self.draft_picks['position'] = self.draft_picks['player_id'].apply(
            lambda x: self.response.player_info(playerId=x).position,
            axis=1,
        )
        self.draft_picks['season_id'] = self.season_id
        return self.draft_picks

    def get_teams(self, key_map=None):
        """
        Parse the teams for the season
        :param key_map: A dictionary representing the values to be parsed from the raw teams data
        :return: A DataFrame representing the teams
        """
        if key_map:
            self.teams = format_response(self.team_objs, key_map)
        self.teams = pd.DataFrame(self.teams)
        self.teams['season_id'] = self.season_id
        return self.teams

    def get_matchups(self, key_map=None):
        """
        Parse the matchups for the season
        :return: A DataFrame representing the matchups
        """
        for team in self.team_objs:
            team['schedule'] = [opponent.team_id for opponent in team['schedule']]
            team['week'] = [i+1 for i in range(len(team['schedule']))]
        matchups = format_response(self.team_objs, key_map)
        matchups = pd.DataFrame(matchups)
        matchups = matchups.apply(pd.Series.explode)
        matchups['season_id'] = self.season_id
        self.matchups = matchups
        return self.matchups
