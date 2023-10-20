import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import pickle

class Bin:
  url = 'https://db.netkeiba.com/'
  races: pd.DataFrame = pd.DataFrame()
  horses: pd.DataFrame = pd.DataFrame()
  from_year: int

  def __init__(self, output: str='./output/data.pickle', from_year: int=2013):
    self.from_year = from_year
    try:
      with open(output, 'rb') as f:
        (self.races, self.horses) = pickle.load(f)
    except FileNotFoundError:
      pass

  def __fetch_race(self, race_id: str) -> pd.DataFrame:
    url = self.url + 'race/' + race_id
    res = requests.get(url)
    res.encoding = 'EUC-JP'
    df = pd.read_html(url, encoding='EUC-JP')[0]
    df = df.rename(columns=lambda x: x.replace(' ', ''))

    soup = BeautifulSoup(res.text, 'html.parser')
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
    df = pd.read_html(url, encoding='EUC-JP')
    df = df[4] if df.columns[0] == '受賞歴' else df[3]
    df = df.rename(columns=lambda x: x.replace(' ', ''))
    df.index = [horse_id] * len(df)
    return df


