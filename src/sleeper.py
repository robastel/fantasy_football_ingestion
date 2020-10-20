import requests
import json
from statistics import median


class Sleeper:
    def __init__(self, league_id, base_url='https://api.sleeper.app/v1/'):
        """
        Initialize the Sleeper class
        :param league_id: The most recent league ID (Sleeper uses a different league ID for each season).
                          The league ID can be found at the end of the league's website URL.
                          A "league" in Sleeper's nomenclature is actually a single season.
        """
        self.most_recent_league_id = league_id
        self.base_url = base_url
        self.seasons = list()

    def get_all_seasons(self):
        season = self._get_season(self.most_recent_league_id)
        self.seasons.append(season)
        while league_id := season['previous_league_id']:
            season = self._get_season(league_id)
            self.seasons.append(season)
        return self.seasons

    @staticmethod
    def _make_request(url):
        response = requests.get(url)
        response.raise_for_status()
        text = json.loads(response.text)
        return text

    def _get_season(self, league_id):
        url = f'{self.base_url}/league/{league_id}'
        season = self._make_request(url)
        season['matchups'] = self._get_all_regular_season_matchups(season)
        return season

    def _get_week_matchups(self, season, week):
        url = f"{self.base_url}/league/{season['league_id']}/matchups/{week}"
        raw_matchups = self._make_request(url)
        matchups = list()
        matchup_against_median = season['settings'].get('league_average_match')
        if matchup_against_median:
            med = median([m['points'] for m in raw_matchups])
        for manager in raw_matchups:
            for opponent in raw_matchups:
                if manager['roster_id'] != opponent['roster_id']:
                    if manager['matchup_id'] == opponent['matchup_id']:
                        matchups.append(
                            {
                                'week': week,
                                'matchup_id': manager['matchup_id'],
                                'roster_id': manager['roster_id'],
                                'points': manager['points'],
                                'opponent_roster_id': opponent['roster_id'],
                                'opponent_points': opponent['points']
                            }
                        )
            if matchup_against_median:
                matchups.append(
                    {
                        'week': week,
                        'matchup_id': manager['matchup_id'],
                        'roster_id': manager['roster_id'],
                        'points': manager['points'],
                        'opponent_roster_id': 'median',
                        'opponent_points': med
                    }
                )
        return matchups

    def _get_all_regular_season_matchups(self, season):
        start_week = season['settings']['start_week']
        playoff_start_week = season['settings']['playoff_week_start']
        last_completed_week = season['settings']['last_scored_leg']
        matchups = list()
        for week in range(start_week, min(last_completed_week+1, playoff_start_week)):
            week_matchups = self._get_week_matchups(season, week)
            matchups.extend(week_matchups)
        return matchups



