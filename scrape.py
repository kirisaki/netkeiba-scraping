import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import pickle
from datetime import datetime
import io
from decimal import Decimal

class Bin:
  url = 'https://db.netkeiba.com/'
  races: pd.DataFrame = pd.DataFrame()
  horses: pd.DataFrame = pd.DataFrame()
  race_profiles: pd.DataFrame = pd.DataFrame()
  invalid_race_ids: set[str] = set()
  from_year: int
  output: str

  def __init__(self, output: str='./output/data.pickle', from_year: int=2013):
    self.from_year = from_year
    self.output = output
    try:
      with open(output, 'rb') as f:
        data = pickle.load(f)
        self.races = data['races']
        self.horses = data['horses']
        self.race_profiles = data['race_profiles']
        self.invalid_race_ids = data['invalid_race_ids']
    except FileNotFoundError:
      pass
    except KeyError:
      pass
    self.update()

  def update(self):
    # fetch races
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
    total_races = len(race_ids)
    race_id_list = sorted(list(race_ids))
    print('')
    for n, id in enumerate(race_id_list):
      print('\r' + 'race({}): {}/{}'.format(id, str(n + 1), str(total_races)), end='')
      try:
        (race, profile) = self.__fetch_race(id)
        races.append(race)
        profiles.append(profile)
      except IndexError:
        self.invalid_race_ids.add(id)
        continue
      except AttributeError:
        self.invalid_race_ids.add(id)
        continue
      else:
        if n % 100 == 0:
          self.races = pd.concat([self.races] + races)
          races = []
          self.race_profiles = pd.concat([self.race_profiles] + profiles)
          profiles = []
          self.save()
    self.races = pd.concat([self.races] + races)
    self.race_profiles = pd.concat([self.race_profiles] + profiles)
    self.save()

    # fetch horses
    horse_ids = set(self.races['horse_id']) - set(self.horses.index)
    horse_id_list = sorted(list(horse_ids))
    total_horses = len(horse_ids)
    horses = []
    print('')
    for n, id in enumerate(horse_id_list):
      print('\r' + 'horse({}): {}/{}'.format(id, str(n + 1), str(total_horses)), end='')
      horse = self.__fetch_horse(id)
      horses.append(horse)
      if n % 100 == 0:
        self.horses = pd.concat([self.horses] + horses)
        horses = []
        self.save()

    self.horses = pd.concat([self.horses] + horses)
    self.save()

  def save(self):
    with open(self.output, 'wb') as f:
      pickle.dump({
          'races': self.races,
          'horses': self.horses,
          'race_profiles': self.race_profiles,
          'invalid_race_ids': self.invalid_race_ids,
        }, f)

  def __fetch_race(self, race_id: str) -> pd.DataFrame:
    url = self.url + 'race/' + race_id
    res = requests.get(url)
    res.encoding = 'EUC-JP'
    profile = pd.DataFrame(
      columns=['title', 'course_type', 'course_length', 'weather', 'going', 'start', 'race_class', 'requirements'])

    # build a race profile
    row = {}
    soup = BeautifulSoup(res.text, 'html5lib')
    data_intro = soup.find('div', attrs={'class': 'data_intro'})
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

    # build a race result
    result = soup.find('table', attrs={'summary': 'レース結果'})
    horse_id_list = []
    horse_a_list = result.find_all(
      'a', attrs={'href': re.compile('^/horse')}
    )
    for a in horse_a_list:
      horse_id = re.findall(r'\d+', a['href'])
      horse_id_list.append(horse_id[0])
    jockey_id_list = []
    jockey_a_list = result.find_all(
      'a', attrs={'href': re.compile('^/jockey')}
    )
    for a in jockey_a_list:
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
      '馬体重':'weight',
      '賞金(万円)': 'prise',
    }, inplace=True)

    race['carry'] = race['carry'].map(Decimal)
    race['prise'] = race['prise'].fillna(0).map(Decimal)

    race['age'] = race['sex_age'].map(lambda x: int(x[1:]))
    race['sex'] = race['sex_age'].map(lambda x: x[0])
    race.drop(columns=['sex_age'], inplace=True)
    race['carry'] = race['carry'].map(float)
    race['last'] = race['last'].map(float)

    def parse_lap(x: str) -> float:
      xs = x.split(':')
      return float(xs[0]) * 60.0 + float(xs[1])
    race['lap'] = race['lap'].fillna('0').map(parse_lap)

    def parse_margin(x: str) -> float:
      x = str(x)
      if x == '0':
        return 0
      elif x == '同着':
        return 0
      elif x == 'ハナ':
        return 1/16
      elif x == 'アタマ':
        return 1/8
      elif x == 'クビ':
        return 1/4
      elif x == '1/2':
        return 1/2
      elif x == '1/4':
        return 1/4
      elif x == '3/4':
        return 3/4
      elif x == '大':
        return 11
      else:
        xs = re.split(r'[.+]', x)
        if len(xs) == 1:
          return float(x[0])
        else:
          return parse_margin(xs[0]) + parse_margin(xs[1])
    race['margin'] = race['margin'].fillna('0').map(parse_margin)

    race['order_during_race'] = list(race['order_during_race'].map(lambda x: map(int, x.split('-'))))
    race['win_odds'] = race['win_odds'].map(float)
    race['weight_diff'] = race['weight'].map(lambda x: int(x.split('(')[1][:-1]))
    race['weight'] = race['weight'].map(lambda x: int(x.split('(')[0]))
    race['horse_id'] = horse_id_list
    race['jockey_id'] = jockey_id_list
    race['race_id'] = [race_id] * len(race)

    return (race, profile)

  def __fetch_horse(self, horse_id: str) -> pd.DataFrame:
    url = self.url + 'horse/' + horse_id
    res = requests.get(url)
    res.encoding = 'EUC-JP'
    row = {}
    soup = BeautifulSoup(res.text, 'html5lib')
    row['name'] = soup.find('div', attrs={'class': 'horse_title'}).find('h1').text
    horse = pd.DataFrame(columns=['name'])
    horse.loc[horse_id] = row
    return horse

if __name__ == '__main__':
  data = Bin(from_year=2020)
