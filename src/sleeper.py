from statistics import median

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
        self.season = None
        self.start_week = None
        self.playoff_start_week = None
        self.last_completed_week = None
        self.matchup_against_median_setting = None
        self.matchup_dfs = dict()
        self.draft = None
        self.rosters = None
        self.roster_to_display_name_map = None
        self.users = None

    def __call__(self):
        self.get_season()
        self.get_settings()
        self.get_draft()
        self.get_rosters()

    def get_season(self):
        url = f'{BASE_URL}/league/{self.season_id}'
        self.season = api_get_request(url)

    def get_settings(self):
        self.start_week = self.season['settings']['start_week']
        self.playoff_start_week = self.season['settings']['playoff_week_start']
        self.last_completed_week = self.season['settings']['last_scored_leg']
        self.matchup_against_median_setting = self.season['settings'].get('league_average_match')

    def get_draft(self):
        self.draft = Draft(self.season['draft_id'])
        self.draft.get_draft_picks()

    def get_rosters(self):
        url = f'{BASE_URL}/league/{self.season_id}/rosters'
        rosters = api_get_request(url)
        roster_to_user_map = {r['roster_id']: r['owner_id'] for r in rosters}
        self.get_users()
        user_to_display_name_map = {u['user_id']: u['display_name'] for u in self.users}
        self.roster_to_display_name_map = {
            r['roster_id']: user_to_display_name_map[roster_to_user_map[r['roster_id']]]
            for r in rosters
        }
        for r in rosters:
            r['manager_display_name'] = user_to_display_name_map[roster_to_user_map[r['roster_id']]]
        self.rosters = rosters

    def get_users(self):
        url = f'{BASE_URL}/league/{self.season_id}/users'
        self.users = api_get_request(url)

    def get_all_completed_matchups(self):
        self._get_winners_bracket()
        for week in range(self.start_week, self.last_completed_week+1):
            week_matchups = self._get_week_matchups(week)
            for k, v in week_matchups.items():
                self.matchup_dfs[k] = pd.concat([self.matchup_dfs.get(k), v])
        for k, v in self.matchup_dfs.items():
            self.matchup_dfs[k] = v.reset_index(drop=True)

    def _get_week_matchups(self, week):
        url = f'{self.base_url}/league/{self.season_id}/matchups/{week}'
        week_matchups = api_get_request(url)
        week_matchups = self._filter_week_matchups(week, week_matchups)
        week_matchups_h2h_df = self._get_week_matchups_h2h(week_matchups)
        week_matchups_median_df = self._get_week_matchups_median(week, week_matchups)
        return {'h2h': week_matchups_h2h_df, 'median': week_matchups_median_df}

    def _filter_week_matchups(self, week, matchups):
        matchups = [
            {
                'week': week,
                'matchup_id': m['matchup_id'],
                'roster_id': m['roster_id'],
                'points': round(m['custom_points'] or m['points'], 2),
                'type': matchup_type
            }
            for m in matchups
            if (matchup_type := self._get_matchup_type(week, m['roster_id']))
        ]
        return matchups

    @staticmethod
    def _get_week_matchups_h2h(matchups):
        matchups_h2h_df = pd.DataFrame(matchups)
        matchups_h2h_df['matchup_id'] = matchups_h2h_df['matchup_id'].astype('Int64')
        matchups_h2h_df = pd.merge(
            matchups_h2h_df, matchups_h2h_df, how='inner', on=['matchup_id'], suffixes=('', '_opp')
        )
        matchups_h2h_df = matchups_h2h_df.drop(['week_opp', 'type_opp'], axis=1)
        matchups_h2h_df = matchups_h2h_df.loc[matchups_h2h_df['roster_id'] != matchups_h2h_df['roster_id_opp']]
        matchups_h2h_df = matchups_h2h_df.sort_values(['matchup_id', 'roster_id'])
        return matchups_h2h_df

    def _get_week_matchups_median(self, week, matchups):
        matchups_median_df = pd.DataFrame()
        if self.matchup_against_median_setting and week < self.playoff_start_week:
            week_median = round(median([m['points'] for m in matchups]), 2)
            median_matchups = [
                {
                    'week': m['week'],
                    'roster_id': m['roster_id'],
                    'points': m['points'],
                    'week_median_points': week_median,
                    'type': m['type']
                }
                for m in matchups
            ]
            matchups_median_df = pd.DataFrame(median_matchups)
            matchups_median_df = matchups_median_df.sort_values('roster_id')
        return matchups_median_df

    def _get_matchup_type(self, week, roster_id):
        matchup_type = None
        if week < self.playoff_start_week:
            matchup_type = 'Regular Season'
        else:
            winners_bracket = self._get_winners_bracket()
            for matchup in winners_bracket:
                if week == matchup['week'] and roster_id in matchup['roster_ids']:
                    matchup_type = matchup['type']
                    break
        return matchup_type

    def _get_winners_bracket(self):
        url = f'{self.base_url}/league/{self.season_id}/winners_bracket'
        raw_winners_bracket = api_get_request(url)
        winners_bracket = self._enrich_winners_bracket(raw_winners_bracket)
        return winners_bracket

    def _enrich_winners_bracket(self, raw_winners_bracket):
        winners_bracket = list()
        for matchup in raw_winners_bracket:
            playoff_round = matchup['r']
            playoff_week = self._get_playoff_week(playoff_round)
            if self.last_completed_week < playoff_week:
                pass
            elif matchup_type := self._get_winners_bracket_matchup_type(matchup):
                matchup['type'] = matchup_type
                matchup['week'] = playoff_week
                matchup['roster_ids'] = [matchup['t1'], matchup['t2']]
                winners_bracket.append(matchup)
        return winners_bracket

    def _get_playoff_week(self, playoff_round):
        playoff_week = self.playoff_start_week - 1 + playoff_round
        return playoff_week

    @staticmethod
    def _get_winners_bracket_matchup_type(winners_bracket_matchup):
        matchup_type = None
        if not (placing_index := winners_bracket_matchup.get('p')):
            matchup_type = f"Round {winners_bracket_matchup['r']}"
        elif placing_index == 1:
            matchup_type = 'Gold Medal Match'
        elif placing_index == 3:
            matchup_type = 'Bronze Medal Match'
        return matchup_type


class User:
    def __init__(self, user_id):
        self.user_id = user_id
        self.user = None

    def get_user(self):
        url = f'{BASE_URL}/user/{self.user_id}'
        self.user = api_get_request(url)


class Draft:
    def __init__(self, draft_id):
        self.draft_id = draft_id
        self.draft_picks = None

    def get_draft_picks(self):
        self.draft_picks = api_get_request(f'{BASE_URL}/draft/{self.draft_id}/picks')

