from time import sleep
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import pickle
from datetime import datetime
import io

class Bin:
  url = 'https://db.netkeiba.com/'
  races: pd.DataFrame = pd.DataFrame()
  horses: pd.DataFrame = pd.DataFrame()
  invalid_race_ids: set[str] = set()
  invalid_horse_ids: set[str] = set()
  from_year: int
  output: str

  def __init__(self, output: str='./output/data.pickle', from_year: int=2013):
    self.from_year = from_year
    self.output = output
    try:
      with open(output, 'rb') as f:
        (self.races, self.horses, self.invalid_race_ids, self.invalid_horse_ids) = pickle.load(f)
    except FileNotFoundError:
      pass
    self.update()

  def update(self):
    year = datetime.now().year
    race_ids = {
      str(y).zfill(4) + str(p).zfill(2) + str(t).zfill(2) + str(d).zfill(2) + str(r).zfill(2)
      for y in range(self.from_year, year + 1)
      for p in range(1, 11)
      for t in range(1, 7)
      for d in range(1, 13)
      for r in range(1, 13)
    } - set(self.races.index) - self.invalid_race_ids
    races = []
    for id in sorted(list(race_ids)):
      print(id)
      try:
        df = self.__fetch_race(id)
        races.append(df)
      except IndexError:
        self.invalid_race_ids.add(id)
        continue
      except AttributeError:
        self.invalid_race_ids.add(id)
        continue
    self.races = pd.concat([self.races] + races)

    horse_ids = set(self.races['horse_id']) - set(self.horses.index) - self.invalid_horse_ids
    horses = []
    for id in sorted(list(horse_ids)):
      try:
        df = self.__fetch_horse(id)
        horses.append(df)
      except IndexError:
        self.invalid_horse_ids.add(id)
        continue
      except AttributeError:
        self.invalid_horse_ids.add(id)
        continue
    self.horses = pd.concat([self.horses] + horses)
    with open(self.output, 'wb') as f:
      pickle.dump((self.races, self.horses, self.invalid_race_ids, self.invalid_horse_ids), f)

  def __fetch_race(self, race_id: str) -> pd.DataFrame:
    url = self.url + 'race/' + race_id
    res = requests.get(url)
    res.encoding = 'EUC-JP'
    f = io.StringIO(res.text)
    df = pd.read_html(f)[0]
    df = df.rename(columns=lambda x: x.replace(' ', ''))

    soup = BeautifulSoup(res.text, 'html5lib')
    texts = (
      soup.find('div', attrs={'class': 'data_intro'}).find_all('p')[0].text
      + soup.find('div', attrs={'class': 'data_intro'}).find_all('p')[1].text
    )
    info = re.findall(r'\w+', texts)
    for text in info:
      if text in ['芝', 'ダート']:
        df['race_type'] = [text] * len(df)
      if '障' in text:
        df['race_type'] = ['障害'] * len(df)
      if 'm' in text:
        df['course_len'] = [int(re.findall(r'\d+', text)[-1])] * len(df)
      if text in ['良', '稍重', '重', '不良']:
        df['ground_state'] = [text] * len(df)
      if text in ['曇', '晴', '雨', '小雨', '小雪', '雪']:
        df['weather'] = [text] * len(df)
      if '年' in text:
        df['date'] = [text] * len(df)

    horse_id_list = []
    horse_a_list = soup.find('table', attrs={'summary': 'レース結果'}).find_all(
      'a', attrs={'href': re.compile('^/horse')}
    )
    for a in horse_a_list:
      horse_id = re.findall(r'\d+', a['href'])
      horse_id_list.append(horse_id[0])
    jockey_id_list = []
    jockey_a_list = soup.find('table', attrs={'summary': 'レース結果'}).find_all(
      'a', attrs={'href': re.compile('^/jockey')}
    )
    for a in jockey_a_list:
      jockey_id = re.findall(r'\d+', a['href'])
      jockey_id_list.append(jockey_id[0])
    df['horse_id'] = horse_id_list

    df['jockey_id'] = jockey_id_list
    df.index = [race_id] * len(df)
    return df

  def __fetch_horse(self, horse_id: str) -> pd.DataFrame:
    url = self.url + 'horse/' + horse_id
    res = requests.get(url)
    res.encoding = 'EUC-JP'
    f = io.StringIO(res.text)
    df = pd.read_html(f)[3]
    df = df.rename(columns=lambda x: x.replace(' ', ''))
    df.index = [horse_id] * len(df)
    return df

if __name__ == '__main__':
  data = Bin(from_year=2023)
