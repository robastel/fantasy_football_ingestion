import pandas as pd

from espn_api.football import League

from src.utils import format_response


class ESPN:
    def __init__(self, league_id, s2, swid, year):
        self.league_id = league_id
        self.s2 = s2
        self.swid = swid
        self.year = year
        self.season = None
        self.teams = None
        self.draft_picks = None
        self.player_map = None

    def get_season(self, year, key_map=None):
        response = League(league_id=self.league_id, year=year, espn_s2=self.s2, swid=self.swid)
        response['settings'] = vars(response.settings)
        self.teams = response.teams
        self.draft_picks = response.draft
        self.player_map = response.player_map
        if key_map:
            response = format_response(response, key_map)
        self.season = pd.DataFrame([response])
        return self.season

    def get_draft_picks(self, key_map=None):
        self.draft_picks['team'] = vars(self.draft_picks['team'])
        for pick in self.draft_picks:
            pick['team'] = vars(pick['team'])
        if key_map:
            self.draft_picks = format_response(self.draft_picks, key_map)
        self.draft_picks = pd.DataFrame(self.draft_picks)
        return self.draft_picks



