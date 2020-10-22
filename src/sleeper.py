from statistics import median

import pandas as pd

from src.utils import api_get_request

BASE_URL = 'https://api.sleeper.app/v1'

class Season:
    def __init__(self, season_id, base_url=BASE_URL):
        """
        Initialize the Sleeper class
        :param league_id: The most recent league ID (Sleeper uses a different league ID for each season).
                          The league ID can be found at the end of the league's website URL.
                          A "league" in Sleeper's nomenclature is actually a single season.
        """
        self.season_id = season_id
        self.base_url = base_url
        self.season = None
        self.start_week = None
        self.playoff_start_week = None
        self.playoff_weeks = None
        self.last_completed_week = None
        self.winners_bracket = None
        self.winners_bracket_type_lookup = None
        self.matchup_against_median_setting = None
        self.matchups_h2h = pd.DataFrame()
        self.matchups_median = pd.DataFrame()

    def __call__(self):
        url = f'{BASE_URL}/league/{self.season_id}'
        self.season = api_get_request(url)
        self.start_week = self.season['settings']['start_week']
        self.playoff_start_week = self.season['settings']['playoff_week_start']
        self.last_completed_week = self.season['settings']['last_scored_leg']
        self.matchup_against_median_setting = self.season['settings'].get('league_average_match')
        self.winners_bracket = self._get_winners_bracket()
        self.playoff_weeks = sorted(list(set(
            [self._get_playoff_week(matchup['r']) for matchup in self.winners_bracket]
        )))
        self.winners_bracket_type_lookup = self._get_winners_bracket_type_lookup()
        self._get_all_matchups()

    def _get_all_matchups(self):
        for week in range(self.start_week, self.last_completed_week+1):
            self._get_week_matchups(week)
        self.matchups_h2h = self.matchups_h2h.sort_values(['week', 'matchup_id', 'roster_id'])
        self.matchups_h2h = self.matchups_h2h.reset_index(drop=True)
        self.matchups_median = self.matchups_median.sort_values(['week', 'roster_id'])
        self.matchups_median = self.matchups_median.reset_index(drop=True)

    def _get_week_matchups(self, week):
        url = f"{self.base_url}/league/{self.season['league_id']}/matchups/{week}"
        matchups = api_get_request(url)
        matchups = [
            {
                'week': week,
                'matchup_id': m['matchup_id'],
                'roster_id': m['roster_id'],
                'points': round(m['custom_points'] or m['points'], 2),
                'type': matchup_type
            }
            for m in matchups
            if (
                matchup_type := self.winners_bracket_type_lookup[week].get(m['roster_id'])
                if week >= self.playoff_start_week else 'Regular Season'
            )
        ]

        matchups_h2h = pd.DataFrame(matchups)
        matchups_h2h['matchup_id'] = matchups_h2h['matchup_id'].astype('Int64')
        matchups_h2h = pd.merge(matchups_h2h, matchups_h2h, how='inner', on=['matchup_id'], suffixes=('', '_opp'))
        matchups_h2h = matchups_h2h.drop(['week_opp', 'type_opp'], axis=1)
        matchups_h2h = matchups_h2h.loc[matchups_h2h['roster_id'] != matchups_h2h['roster_id_opp']]
        self.matchups_h2h = pd.concat([self.matchups_h2h, matchups_h2h])

        if self.matchup_against_median_setting and week < self.playoff_start_week:
            week_median = round(median([m['points'] for m in matchups]), 2)
            matchups_median = matchups_h2h.copy()
            matchups_median['week_median_points'] = week_median
            matchups_median = matchups_median[['week', 'roster_id', 'points', 'week_median_points', 'type']]
            self.matchups_median = pd.concat([self.matchups_median, matchups_median])

    def _get_winners_bracket(self):
        if self.last_completed_week >= self.playoff_start_week:
            url = f"{self.base_url}/league/{self.season['league_id']}/winners_bracket"
            # Winners bracket structure (from Sleeper website: https://docs.sleeper.app/#getting-the-playoff-bracket)
            # r: (int) The round for this matchup, 1st, 2nd, 3rd round, etc.
            # m: (int) The match id of the matchup, unique for all matchups within a bracket.
            # t1: (int) The roster_id of a team in this matchup OR {w: 1} which means the winner of match id 1
            # t2: (int) The roster_id of the other team in this matchup OR {l: 1} which means the loser of match id 1
            # w: (int) The roster_id of the winning team, if the match has been played.
            # l: (int) The roster_id of the losing team, if the match has been played.
            # t1_from: (object)	Where t1 comes from, either winner or loser of the match id.
            # t2_from: (object)	Where t2 comes from, either winner or loser of the match id.
            winners_bracket = api_get_request(url)
            winners_bracket = [
                matchup for matchup in winners_bracket
                if matchup.get('p', 0) <= 3
                and self.last_completed_week >= self._get_playoff_week(matchup['r'])
            ]
            return winners_bracket
        else:
            return list()

    def _get_playoff_week(self, playoff_round):
        playoff_week = self.playoff_start_week - 1 + playoff_round
        return playoff_week

    def _get_winners_bracket_type_lookup(self):
        winners_bracket_type_lookup = {week: dict() for week in self.playoff_weeks}
        for matchup in self.winners_bracket:
            playoff_round = matchup['r']
            placing_index = matchup.get('p')
            if placing_index == 1:
                matchup_type = 'Championship'
            elif placing_index == 3:
                matchup_type = 'Bronze Medal Game'
            else:
                matchup_type = f'Round {playoff_round}'
            week = self._get_playoff_week(playoff_round)
            teams = ['t1', 't2']  # This nomenclature comes from the Sleeper API
            for team in teams:
                winners_bracket_type_lookup[week][matchup[team]] = matchup_type
        return winners_bracket_type_lookup

