from src.utils import get_logger, parse_args
from src.sleeper import Season, User

logger = get_logger('Fantasy Football Stats')
args_config = [
    {
        'definition': ['-s', '--sleeper'],
        'params': {
            'help': 'Most recent Sleeper season league ID'
        }
    }
]
args = parse_args(args_config)


if __name__ == '__main__':
    seasons = list()
    s = Season(args['sleeper'])
    while s.season_id:
        s()
        seasons.append(s)
        s = Season(s.season['previous_league_id'])

    for season in seasons:
        logger.info(f"Season: {season.season['season']}, Last Completed Week: {season.last_completed_week}")
