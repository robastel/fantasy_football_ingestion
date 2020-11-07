import pandas as pd

from espn_api.football import League

from src.utils import format_response


class ESPNSeason:
    def __init__(self, league_id, s2, swid, year):
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
        for pick in self.draft_picks:
            pick['team'] = vars(pick['team'])
        if key_map:
            self.draft_picks = format_response(self.draft_picks, key_map)
        self.draft_picks = pd.DataFrame(self.draft_picks)
        self.draft_picks['year'] = self.year
        return self.draft_picks

    def get_teams(self, key_map=None):
        if key_map:
            self.teams = format_response(self.team_objs, key_map)
        self.teams = pd.DataFrame(self.teams)
        self.teams['year'] = self.year
        return self.teams

    def get_matchups(self, key_map=None):
        matchups = list()
        for team in self.team_objs:
            team['schedule'] = [opponent.team_id for opponent in team['schedule']]
            for i, opponent_id in enumerate(team['schedule']):
                matchups.append({
                    'year': self.year,
                    'week': i+1,
                    'team_id': team['team_id'],
                    'points': team['scores'][i],
                    'margin_of_victory': round(team['mov'][i], 2),
                    'opponent_id': opponent_id
                })
        self.matchups = pd.DataFrame(matchups)
        return self.matchups
