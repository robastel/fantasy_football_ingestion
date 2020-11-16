import pandas as pd

from espn_api.football import League

from src.utils import format_response


class ESPNSeason:
    def __init__(self, league_id, s2, swid, year):
        """
        Initialize the ESPNSeason class
        :param league_id: The ESPN league ID for which this season is a part of
        :param s2: Your ESPN s2 cookie (used for authentication)
        :param swid: Your ESPN swid cookie (used for authentication)
        :param year: The year of the season
        """
        self.league_id = league_id
        self.s2 = s2
        self.swid = swid
        self.year = year
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
        response = League(league_id=self.league_id, year=self.year, espn_s2=self.s2, swid=self.swid)
        response = vars(response)
        response['settings'] = vars(response['settings'])
        self.start_week = response.get('firstScoringPeriod', 1)
        self.regular_season_weeks = response['settings'].get('reg_season_count')
        self.team_objs = [vars(team) for team in response['teams']]
        self.draft_picks = [vars(pick) for pick in response['draft']]
        self.player_map = response['player_map']
        if key_map:
            response = format_response(response, key_map)
        self.season = pd.DataFrame([response])
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
        self.draft_picks['year'] = self.year
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
        self.teams['year'] = self.year
        return self.teams

    def get_matchups(self, key_map=None):
        """
        Parse the matchups for the season
        :return: A DataFrame representing the matchups
        """
        matchups = list()
        for team in self.team_objs:
            if key_map:
                team_matchups = format_response(team, key_map)
                team_matchups['schedule'] = [opponent.team_id for opponent in team['schedule']]
        matchups = pd.DataFrame(matchups)
        matchups['year'] = self.year
        self.matchups = matchups
        return self.matchups
