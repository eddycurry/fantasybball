import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import urllib
import re
import string
from itertools import cycle

import asyncio
import aiohttp
import requests
import json
import async_timeout
import tqdm
from timeit import default_timer as timer

class NBAGamelog:
    """
    Pulls data from stats.nba.com API. Functionality to pull down player info and
    per year stats asynchronously.

    :param min_year: Integer, minimum year to pull data for (inclusive)
    :param max_year: Integer, maximum year to pull data for (inclusive)
    """
    def __init__(self, min_year, max_year):
        self.min_year = min_year
        self.max_year = max_year
        self.header = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'stats.nba.com',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
            'x-nba-stats-origin': 'stats',
            'x-nba-stats-token': 'true',
            'X-NewRelic-ID': 'VQECWF5UChAHUlNTBwgBVw=='
        }

    def get_player_roster(self):
        """
        Pulls down all players and seasons they played in.

        Returns: Pandas DataFrame, player season information
        """
        # parameters for API call
        # Necessary to pass a season, but all seasons are returned
        params = {
            'IsOnlyCurrentSeason':'0',
            'LeagueID':'00',
            'Season':'2015-16'
        }
        try:
            t = requests.get('https://stats.nba.com/stats/commonallplayers', params=params,
                     headers=self.header).json()
        except:
            raise Exception('Player Roster Pull Failed')

        # unpack information from json
        player_list = t['resultSets'][0]['rowSet']

        df_players = pd.DataFrame(player_list, columns = t['resultSets'][0]['headers'])

        # data is currently constructed so that each player has one row.
        # extend df so that each player-season is a row
        def extend_df(df):
            hold = []
            for row in df.itertuples():
                for year in range(int(row.FROM_YEAR), int(row.TO_YEAR)+1):
                    hold.append([row.PERSON_ID, row.DISPLAY_FIRST_LAST, year])
            return pd.DataFrame(hold, columns = ['PERSON_ID', 'PLAYER_NAME', 'SEASON'])

        df_ext = extend_df(df_players)

        #return data for seasons specified
        return df_ext[(df_ext.SEASON >= self.min_year) & (df_ext.SEASON <= self.max_year)]

    @staticmethod
    def get_proxies():
        """
        Get a pool of free proxies to cycle through for requests.
        Otherwise, stats.nba.com will block your IP address.

        Returns: List, proxies to be used for requests
        """
        url = 'https://free-proxy-list.net/'
        response = requests.get(url)
        soup2 = BeautifulSoup(response.text, 'html.parser')
        ips = []
        for link in soup2.find_all('tr'):
            try:
                ips.append(':'.join(list(map(lambda x: x.text, link.find_all('td')[:2]))))
            except:
                continue
        return list(filter(lambda x: re.search('^[\d\.\:]+$', x), ips))


    async def fetch(self, session, row, url, params, gamelog):
        # parameters for API call
        # create copy so original params object is not modified for other calls
        params_copy = params.copy()

        #set PlayerID for params to the current player
        params_copy['PlayerID'] = int(row.PERSON_ID)

        if gamelog:
            params_copy['Season'] = "{}-{}".format(row.SEASON, str(row.SEASON+1)[2:])

        # get proxies and create a cyclical iterator
        proxies = self.get_proxies()
        proxy_pool = cycle(proxies)
        proxy = next(proxy_pool)
        proxy_test = True

        test_count = 0
        while(proxy_test):
            try:
                async with session.get(url,
                                       proxy='http://' + proxy,
                                       headers=self.header,
                                       params=params_copy,
                                       timeout=3) as response:
                    response_content = await response.json()
                    proxy_test=False

            except:
                proxy = next(proxy_pool)
                test_count+=1
                await asyncio.sleep(np.random.uniform(0,.5))
            # stop requesting if 300 attempts have been made
            if test_count > 300:
                print(str(player_id))
                response_content ={'resultSets':[{'rowSet':[]}, {'rowSet':[]}]}
                proxy_test = False
        return response_content

    async def bound_fetch(self, sem, session, row, url, params, gamelog):
    # function with semaphore.
        async with sem:
            return await self.fetch(session, row, url, params, gamelog)

    async def run(self, df_ext, semaphore, url, params, gamelog=False):
        tasks = []
        sem = asyncio.Semaphore(semaphore)
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # iterate through players and make requests for each
            # for row in df_ext.itertuples():
            #     task = asyncio.ensure_future(self.bound_fetch(sem, session, row, url, params, gamelog))
            #     tasks.append(task)
            responses = []
            tasks = [asyncio.ensure_future(self.bound_fetch(sem, session, row, url, params, gamelog)) for row in df_ext.itertuples()]
            for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                responses.append(await f)

            #responses = await asyncio.gather(*tasks)
            return responses

    def get_player_peryear(self, semaphore):
        """
        Pull down player per year statistics

        :param semaphore: Integer, number of concurrent jobs allowed

        Returns: Pandas DataFrame, player per year statistics
        """
        start = timer()
        df_ext = self.get_player_roster()

        #keys required for API request
        params = {
            'DateFrom':'',
            'DateTo':'',
            'GameSegment': '',
            'LastNGames': 0,
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Base',
            'Month': 0,
            'OpponentTeamID': 0,
            'Outcome':'',
            'PORound': 0,
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': 0,
            'Season': '2000-01',
            'PlusMinus': 'N',
            'Rank': 'N',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'Split':'yoy',
            'VsConference':'',
            'VsDivision':''
        }

        url = 'https://stats.nba.com/stats/playerdashboardbyyearoveryear/'

        # code to generate and execute asynchronous requests
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run(df_ext.drop_duplicates(subset=['PERSON_ID']), semaphore, url, params))
        loop.run_until_complete(future)

        # unpack json data and append person_id
        results = filter(lambda x: len(x) > 0, map(lambda x: x['resultSets'][1]['rowSet'], future.result()))
        player_id = [[i['parameters']['PlayerID']] * len(i['resultSets'][1]['rowSet']) for i in future.result()]
        player_id = [season for player in player_id for season in player]
        columns = future.result()[0]['resultSets'][1]['headers']

        df = pd.DataFrame([game for season in results for game in season], columns=columns)
        df['PERSON_ID'] = player_id

        end = timer()
        print(end - start)

        return df

    def get_player_info(self, semaphore):
        """
        Pull down player info, like height, birthdate, and draft position

        :param semaphore: Integer, number of concurrent jobs allowed

        Returns: Pandas DataFrame, player info
        """

        start = timer()

        df_ext = self.get_player_roster()

        url = 'https://stats.nba.com/stats/commonplayerinfo/'
        params = {}

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run(df_ext.drop_duplicates(subset=['PERSON_ID']), semaphore, url, params))
        loop.run_until_complete(future)

        results = filter(lambda x: len(x) > 0, map(lambda x: x['resultSets'][0]['rowSet'], future.result()))
        columns = future.result()[0]['resultSets'][0]['headers']

        end = timer()
        print(end - start)

        return pd.DataFrame([game for season in results for game in season if len(game) > 0], columns=columns)
    
    def get_player_gamelog(self, semaphore):
        """
        Pull down player gamelogs

        :param semaphore: Integer, number of concurrent jobs allowed

        Returns: Pandas DataFrame, player gamelogs
        """

        start = timer()

        df_ext = self.get_player_roster()

        url = 'https://stats.nba.com/stats/playergamelogs'
        params = {
            'DateFrom':'',
            'DateTo':'',
            'GameSegment': '',
            'LastNGames': 0,
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Base',
            'Month': 0,
            'OpponentTeamID': 0,
            'Outcome':'',
            'PORound': 0,
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': 0,
            'PlusMinus': 'N',
            'Rank': 'N',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'Split':'yoy',
            'VsConference':'',
            'VsDivision':''
        }

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run(df_ext, semaphore, url, params, True))
        loop.run_until_complete(future)

        results = filter(lambda x: len(x) > 0, map(lambda x: x['resultSets'][0]['rowSet'], future.result()))
        columns = future.result()[0]['resultSets'][0]['headers']

        end = timer()
        print(end - start)

        return pd.DataFrame([game for season in results for game in season if len(game) > 0], columns=columns)




if __name__=="__main__":
    scraper = NBAGamelog(min_year=2017, max_year=2018)
    df = scraper.get_player_roster()
    df.to_csv('data/player_seasons.csv', index=False)
    print(df.shape)

    # df = scraper.get_player_peryear(semaphore=50)
    # df.to_csv('data/player_peryear.csv', index=False)

    # df = scraper.get_player_info(semaphore=50)
    # df.to_csv('data/player_info.csv', index=False)

    df = scraper.get_player_gamelog(semaphore=50)
    df.to_csv('data/player_gamelog.csv', index=False)
