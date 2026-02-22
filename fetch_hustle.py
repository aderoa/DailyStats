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
import requests
from datetime import datetime, timedelta, timezone

SLEEP = 1.0  # seconds between API calls
MAX_RETRIES = 3
TIMEOUT = 60
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

# Headers required by stats.nba.com — without these it blocks/times out
STATS_HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nba.com/',
    'Origin': 'https://www.nba.com',
    'Connection': 'keep-alive',
}


def get_today_pacific():
    """Get today's date in Pacific time."""
    utc_now = datetime.now(timezone.utc)
    pacific = utc_now - timedelta(hours=8)
    return pacific.strftime('%Y-%m-%d')


def fetch_game_ids(date_str):
    """Get all game IDs for a date using the CDN scoreboard (no auth needed)."""
    print(f'  Scoreboard for {date_str}...')

    # Use CDN schedule (same as the HTML app — reliable, no headers needed)
    url = 'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json'
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        print(f'    Schedule fetch failed: {e}')
        return []

    # Parse date — schedule uses "MM/DD/YYYY 12:00:00 AM" format
    y, m, d = date_str.split('-')
    targets = [
        f'{int(m)}/{int(d)}/{y}',
        f'{m}/{d}/{y}',
        f'{int(m)}/{int(d)}/{y} 12:00:00 AM',
    ]

    ids = []
    for gd in data.get('leagueSchedule', {}).get('gameDates', []):
        gd_date = gd.get('gameDate', '').split(' ')[0]
        if gd_date in targets or gd.get('gameDate') in targets:
            for g in gd.get('games', []):
                status = g.get('gameStatus', 0)
                if status >= 2:  # in progress or final
                    ids.append(g['gameId'])
            break

    print(f'    {len(ids)} completed/live game(s)')
    return ids


def fetch_hustle(game_id):
    """Fetch hustle stats for one game using direct requests with proper headers."""
    print(f'    Hustle for {game_id}...')

    url = 'https://stats.nba.com/stats/boxscorehustlev2'
    params = {'GameID': game_id}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.get(url, params=params, headers=STATS_HEADERS, timeout=TIMEOUT)
            if res.status_code == 200:
                data = res.json()
                break
            print(f'      Attempt {attempt}: HTTP {res.status_code}')
        except requests.exceptions.Timeout:
            print(f'      Attempt {attempt}: Timeout')
        except Exception as e:
            print(f'      Attempt {attempt}: {e}')

        if attempt < MAX_RETRIES:
            wait = 3 * attempt
            print(f'      Retrying in {wait}s...')
            time.sleep(wait)
    else:
        print(f'      FAILED after {MAX_RETRIES} attempts')
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

    print(f'  Saved {len(all_players)} players -> {filepath}')
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

    print(f'\nDone! {success}/{len(dates)} dates processed.')
    if success == 0 and dates:
        sys.exit(1)


if __name__ == '__main__':
    main()
