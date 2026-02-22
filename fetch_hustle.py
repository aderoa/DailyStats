#!/usr/bin/env python3
"""
Fetch NBA hustle stats for a given date and save as JSON.
Designed to run via GitHub Actions or locally.

Usage:
  python fetch_hustle.py                  # Today's games (Pacific time)
  python fetch_hustle.py 2026-02-20       # Specific date
  python fetch_hustle.py 2026-02-15 2026-02-20  # Date range
"""

import sys
import os
import json
import time
from datetime import datetime, timedelta, timezone

from nba_api.stats.endpoints import scoreboardv3, boxscorehustlev2

SLEEP = 0.7  # seconds between API calls
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def get_today_pacific():
    """Get today's date in Pacific time."""
    utc_now = datetime.now(timezone.utc)
    pacific = utc_now - timedelta(hours=8)
    return pacific.strftime('%Y-%m-%d')


def fetch_game_ids(date_str):
    """Get all finished game IDs for a date."""
    print(f'  Scoreboard for {date_str}...')
    sb = scoreboardv3.ScoreboardV3(game_date=date_str, league_id='00')
    data = sb.get_dict()
    games = data.get('scoreboard', {}).get('games', [])
    ids = []
    for g in games:
        status = g.get('gameStatus', 0)
        if status >= 2:  # in progress or final
            ids.append(g['gameId'])
    print(f'    {len(ids)} completed/live game(s)')
    return ids


def fetch_hustle(game_id):
    """Fetch hustle stats for one game. Returns list of player dicts."""
    print(f'    Hustle for {game_id}...')
    try:
        box = boxscorehustlev2.BoxScoreHustleV2(game_id=game_id)
        data = box.get_dict()
    except Exception as e:
        print(f'      ERROR: {e}')
        return []

    players = []
    for rs in data.get('resultSets', []):
        if rs['name'] != 'PlayerStats':
            continue
        headers = rs['headers']
        for row in rs['rowSet']:
            obj = dict(zip(headers, row))
            players.append({
                'personId': obj.get('PLAYER_ID'),
                'name': obj.get('PLAYER_NAME', ''),
                'team': obj.get('TEAM_ABBREVIATION', ''),
                'gameId': game_id,
                'minutes': obj.get('MIN', 0),
                'deflections': obj.get('DEFLECTIONS', 0),
                'looseBalls': obj.get('LOOSE_BALLS_RECOVERED', 0),
                'contested2': obj.get('CONTESTED_SHOTS_2PT', 0),
                'contested3': obj.get('CONTESTED_SHOTS_3PT', 0),
                'contestedTotal': obj.get('CONTESTED_SHOTS', 0),
                'charges': obj.get('CHARGES_DRAWN', 0),
                'screenAst': obj.get('SCREEN_ASSISTS', 0),
                'boxOuts': obj.get('BOX_OUTS', 0),
                'boxOutsOff': obj.get('OFF_BOXOUTS', 0),
                'boxOutsDef': obj.get('DEF_BOXOUTS', 0),
            })
    print(f'      {len(players)} players')
    return players


def process_date(date_str):
    """Fetch hustle stats for all games on a date, save to JSON."""
    print(f'\n{"="*50}')
    print(f'Processing {date_str}')
    print(f'{"="*50}')

    game_ids = fetch_game_ids(date_str)
    if not game_ids:
        print(f'  No completed games for {date_str}')
        return False

    all_players = []
    for gid in game_ids:
        time.sleep(SLEEP)
        players = fetch_hustle(gid)
        all_players.extend(players)

    if not all_players:
        print(f'  No hustle data returned for {date_str}')
        return False

    # Save JSON
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f'hustle_{date_str}.json')
    with open(filepath, 'w') as f:
        json.dump({
            'date': date_str,
            'games': len(game_ids),
            'players': all_players
        }, f, separators=(',', ':'))

    print(f'  ✅ Saved {len(all_players)} players → {filepath}')
    return True


def main():
    if len(sys.argv) == 1:
        dates = [get_today_pacific()]
    elif len(sys.argv) == 2:
        dates = [sys.argv[1]]
    elif len(sys.argv) == 3:
        start = datetime.strptime(sys.argv[1], '%Y-%m-%d')
        end = datetime.strptime(sys.argv[2], '%Y-%m-%d')
        dates = []
        d = start
        while d <= end:
            dates.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
    else:
        print('Usage: python fetch_hustle.py [date] [end_date]')
        sys.exit(1)

    print(f'NBA Hustle Stats Fetcher')
    print(f'Dates: {", ".join(dates)}')

    success = 0
    for date_str in dates:
        if process_date(date_str):
            success += 1
        if len(dates) > 1:
            time.sleep(1)

    print(f'\n✅ Done! {success}/{len(dates)} dates processed.')
    # Exit with error if nothing succeeded (useful for GitHub Actions)
    if success == 0 and dates:
        sys.exit(1)


if __name__ == '__main__':
    main()
