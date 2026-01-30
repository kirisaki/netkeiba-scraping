import io
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .parsers import parse_lap, parse_margin


BASE_URL = 'https://db.netkeiba.com/'
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

BET_TYPE_MAP = {
    '単勝': 'win',
    '複勝': 'place',
    '枠連': 'bracket_quinella',
    '馬連': 'quinella',
    'ワイド': 'quinella_place',
    '馬単': 'exacta',
    '三連複': 'trio',
    '三連単': 'trifecta',
}


class Scraper:
    races: pd.DataFrame
    horses: pd.DataFrame
    race_profiles: pd.DataFrame
    payouts: pd.DataFrame
    invalid_race_ids: set[str]
    from_year: int
    output_dir: Path

    def __init__(self, output_dir: str = './output', from_year: int = 2013):
        self.races = pd.DataFrame()
        self.horses = pd.DataFrame()
        self.race_profiles = pd.DataFrame()
        self.payouts = pd.DataFrame()
        self.invalid_race_ids = set()
        self.from_year = from_year
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self):
        try:
            races_path = self.output_dir / 'races.parquet'
            if races_path.exists():
                self.races = pd.read_parquet(races_path)

            horses_path = self.output_dir / 'horses.parquet'
            if horses_path.exists():
                self.horses = pd.read_parquet(horses_path)

            profiles_path = self.output_dir / 'race_profiles.parquet'
            if profiles_path.exists():
                self.race_profiles = pd.read_parquet(profiles_path)

            payouts_path = self.output_dir / 'payouts.parquet'
            if payouts_path.exists():
                self.payouts = pd.read_parquet(payouts_path)

            invalid_path = self.output_dir / 'invalid_race_ids.json'
            if invalid_path.exists():
                with open(invalid_path) as f:
                    self.invalid_race_ids = set(json.load(f))
        except Exception:
            pass

    def save(self):
        if not self.races.empty:
            self.races.to_parquet(self.output_dir / 'races.parquet')

        if not self.horses.empty:
            self.horses.to_parquet(self.output_dir / 'horses.parquet')

        if not self.race_profiles.empty:
            self.race_profiles.to_parquet(self.output_dir / 'race_profiles.parquet')

        if not self.payouts.empty:
            self.payouts.to_parquet(self.output_dir / 'payouts.parquet')

        with open(self.output_dir / 'invalid_race_ids.json', 'w') as f:
            json.dump(sorted(self.invalid_race_ids), f)

    def update(self):
        self._update_races()
        self._update_payouts()
        self._update_horses()

    def _update_races(self):
        year = datetime.now().year
        race_ids = {
            str(y).zfill(4) + str(p).zfill(2) + str(t).zfill(2) + str(d).zfill(2) + str(r).zfill(2)
            for y in range(self.from_year, year + 1)
            for p in range(1, 11)
            for t in range(1, 7)
            for d in range(1, 13)
            for r in range(1, 13)
        } - set(self.race_profiles.index) - self.invalid_race_ids

        races = []
        profiles = []
        payouts = []
        total_races = len(race_ids)
        race_id_list = sorted(list(race_ids))

        print('')
        for n, id in enumerate(race_id_list):
            print('\r' + 'race({}): {}/{}'.format(id, str(n + 1), str(total_races)), end='')
            try:
                (race, profile, payout) = self._fetch_race(id)
                races.append(race)
                profiles.append(profile)
                payouts.append(payout)
            except (IndexError, AttributeError):
                self.invalid_race_ids.add(id)
                continue
            else:
                if n % 100 == 0:
                    self.races = pd.concat([self.races] + races)
                    races = []
                    self.race_profiles = pd.concat([self.race_profiles] + profiles)
                    profiles = []
                    self.payouts = pd.concat([self.payouts] + payouts)
                    payouts = []
                    self.save()

        self.races = pd.concat([self.races] + races)
        self.race_profiles = pd.concat([self.race_profiles] + profiles)
        self.payouts = pd.concat([self.payouts] + payouts)
        self.save()

    def _update_horses(self):
        horse_ids = set(self.races['horse_id']) - set(self.horses.index)
        horse_id_list = sorted(list(horse_ids))
        total_horses = len(horse_ids)
        horses = []

        print('')
        for n, id in enumerate(horse_id_list):
            print('\r' + 'horse({}): {}/{}'.format(id, str(n + 1), str(total_horses)), end='')
            horse = self._fetch_horse(id)
            horses.append(horse)
            if n % 100 == 0:
                self.horses = pd.concat([self.horses] + horses)
                horses = []
                self.save()

        self.horses = pd.concat([self.horses] + horses)
        self.save()

    def _update_payouts(self):
        existing_payout_race_ids = set(self.payouts['race_id'].unique()) if not self.payouts.empty else set()
        missing_race_ids = set(self.race_profiles.index) - existing_payout_race_ids
        if not missing_race_ids:
            return

        race_id_list = sorted(list(missing_race_ids))
        total = len(race_id_list)
        payouts = []

        print('')
        for n, race_id in enumerate(race_id_list):
            print('\r' + 'payout({}): {}/{}'.format(race_id, str(n + 1), str(total)), end='')
            try:
                payout = self._fetch_payouts(race_id)
                payouts.append(payout)
            except (IndexError, AttributeError):
                continue
            else:
                if n % 100 == 0:
                    self.payouts = pd.concat([self.payouts] + payouts)
                    payouts = []
                    self.save()

        self.payouts = pd.concat([self.payouts] + payouts)
        self.save()

    def _fetch_payouts(self, race_id: str) -> pd.DataFrame:
        url = BASE_URL + 'race/' + race_id
        res = requests.get(url, headers=REQUEST_HEADERS)
        res.encoding = 'EUC-JP'
        soup = BeautifulSoup(res.text, 'html5lib')
        return self._parse_payouts(soup, race_id)

    def _fetch_race(self, race_id: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        url = BASE_URL + 'race/' + race_id
        res = requests.get(url, headers=REQUEST_HEADERS)
        res.encoding = 'EUC-JP'

        profile = pd.DataFrame(
            columns=['title', 'course_type', 'course_length', 'weather', 'going', 'start', 'race_class', 'requirements']
        )

        soup = BeautifulSoup(res.text, 'html5lib')
        data_intro = soup.find('div', attrs={'class': 'data_intro'})

        # レースプロフィールを構築
        row = {}
        row['title'] = data_intro.find('h1').text
        conds = list(map(lambda x: x.strip(), data_intro.find('diary_snap_cut').find('span').text.split('/')))
        row['course_type'] = conds[0][0]
        row['course_length'] = int(conds[0][-5:-1])
        row['weather'] = conds[1][-1]
        row['going'] = conds[2][-1]
        hrs_min = list(map(lambda x: int(x), conds[3][-5:].split(':')))
        detail = list(map(lambda x: x.strip(), data_intro.find('p', attrs={'class': 'smalltxt'}).text.split()))
        yy_remain = detail[0].split('年')
        mm_remain = yy_remain[1].split('月')
        yy = int(yy_remain[0])
        mm = int(mm_remain[0])
        dd = int(mm_remain[1].replace('日', ''))
        row['start'] = datetime(yy, mm, dd, hrs_min[0], hrs_min[1])
        row['race_class'] = detail[2]
        row['requirements'] = detail[3]
        profile.loc[race_id] = row

        # レース結果を構築
        result = soup.find('table', attrs={'summary': 'レース結果'})

        horse_id_list = []
        for a in result.find_all('a', attrs={'href': re.compile('^/horse')}):
            horse_id = re.findall(r'\d+', a['href'])
            horse_id_list.append(horse_id[0])

        jockey_id_list = []
        for a in result.find_all('a', attrs={'href': re.compile('^/jockey')}):
            jockey_id = re.findall(r'\d+', a['href'])
            jockey_id_list.append(jockey_id[0])

        f = io.StringIO(result.decode())
        race = pd.read_html(f)[0]
        race.rename(columns=lambda x: x.replace(' ', ''), inplace=True)
        race.drop(columns=['馬名', '騎手', 'ﾀｲﾑ指数', '人気', '調教ﾀｲﾑ', '厩舎ｺﾒﾝﾄ', '備考', '調教師', '馬主'], inplace=True)
        race.rename(columns={
            '着順': 'order',
            '枠番': 'position',
            '馬番': 'number',
            '性齢': 'sex_age',
            '斤量': 'carry',
            'タイム': 'lap',
            '着差': 'margin',
            '通過': 'order_during_race',
            '上り': 'last',
            '単勝': 'win_odds',
            '馬体重': 'weight',
            '賞金(万円)': 'prise',
        }, inplace=True)

        def safe_float(x, default=0.0):
            try:
                return float(x)
            except (ValueError, TypeError):
                return default

        def safe_int(x, default=0):
            try:
                return int(x)
            except (ValueError, TypeError):
                return default

        race['order'] = race['order'].map(lambda x: safe_int(x))
        race['position'] = race['position'].map(lambda x: safe_int(x))
        race['number'] = race['number'].map(lambda x: safe_int(x))
        race['carry'] = race['carry'].map(lambda x: safe_float(x))
        race['prise'] = race['prise'].fillna(0).map(lambda x: safe_float(x))
        race['age'] = race['sex_age'].map(lambda x: safe_int(x[1:]) if isinstance(x, str) and len(x) > 1 else 0)
        race['sex'] = race['sex_age'].map(lambda x: x[0] if isinstance(x, str) and len(x) > 0 else '')
        race.drop(columns=['sex_age'], inplace=True)
        race['last'] = race['last'].map(lambda x: safe_float(x))
        race['lap'] = race['lap'].fillna('0').map(parse_lap)
        race['margin'] = race['margin'].fillna('0').map(parse_margin)
        race['order_during_race'] = list(race['order_during_race'].map(
            lambda x: list(map(int, x.split('-'))) if isinstance(x, str) and '-' in x else []
        ))
        race['win_odds'] = race['win_odds'].map(lambda x: safe_float(x))
        race['weight_diff'] = race['weight'].map(
            lambda x: safe_int(x.split('(')[1][:-1]) if isinstance(x, str) and '(' in x else 0
        )
        race['weight'] = race['weight'].map(
            lambda x: safe_int(x.split('(')[0]) if isinstance(x, str) and '(' in x else 0
        )
        race['horse_id'] = horse_id_list
        race['jockey_id'] = jockey_id_list
        race['race_id'] = [race_id] * len(race)

        # 払い戻しを構築
        payout = self._parse_payouts(soup, race_id)

        return (race, profile, payout)

    def _parse_payouts(self, soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
        payout_rows = []

        pay_tables = soup.find_all('table', class_='pay_table_01')
        for table in pay_tables:
            for tr in table.find_all('tr'):
                cells = tr.find_all(['th', 'td'])
                if len(cells) < 3:
                    continue

                bet_type_jp = cells[0].get_text(strip=True)
                if bet_type_jp not in BET_TYPE_MAP:
                    continue

                bet_type = BET_TYPE_MAP[bet_type_jp]

                # brタグで分割してリスト化
                numbers_list = self._split_by_br(cells[1])
                payout_list = self._split_by_br(cells[2])
                popularity_list = self._split_by_br(cells[3]) if len(cells) >= 4 else ['0'] * len(numbers_list)

                # 各結果を処理
                for i, numbers_text in enumerate(numbers_list):
                    numbers = self._parse_numbers(numbers_text)
                    if not numbers:
                        continue

                    payout_text = payout_list[i] if i < len(payout_list) else '0'
                    payout_text = payout_text.replace(',', '').replace('円', '')
                    try:
                        payout = int(payout_text)
                    except ValueError:
                        continue

                    pop_text = popularity_list[i] if i < len(popularity_list) else '0'
                    pop_text = pop_text.replace('人気', '')
                    try:
                        popularity = int(pop_text)
                    except ValueError:
                        popularity = 0

                    payout_rows.append({
                        'race_id': race_id,
                        'bet_type': bet_type,
                        'numbers': numbers,
                        'payout': payout,
                        'popularity': popularity,
                    })

        return pd.DataFrame(payout_rows)

    def _split_by_br(self, cell) -> list[str]:
        """brタグで区切られたセル内容をリストに分割"""
        texts = []
        current = []
        for content in cell.children:
            if hasattr(content, 'name') and content.name == 'br':
                if current:
                    texts.append(''.join(current).strip())
                    current = []
            elif content.string:
                current.append(content.string)
            elif hasattr(content, 'get_text'):
                current.append(content.get_text())
        if current:
            texts.append(''.join(current).strip())
        return texts if texts else [cell.get_text(strip=True)]

    def _parse_numbers(self, text: str) -> list[int]:
        text = text.replace('→', '-').replace(' ', '-').replace('－', '-')
        parts = re.split(r'[-ー]', text)
        numbers = []
        for part in parts:
            part = part.strip()
            if part.isdigit():
                numbers.append(int(part))
        return numbers

    def _fetch_horse(self, horse_id: str) -> pd.DataFrame:
        url = BASE_URL + 'horse/' + horse_id
        res = requests.get(url, headers=REQUEST_HEADERS)
        res.encoding = 'EUC-JP'

        soup = BeautifulSoup(res.text, 'html5lib')
        row = {'name': soup.find('div', attrs={'class': 'horse_title'}).find('h1').text}

        horse = pd.DataFrame(columns=['name'])
        horse.loc[horse_id] = row
        return horse
