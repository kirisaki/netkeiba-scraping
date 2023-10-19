import pandas as pd
import requests
import re
from bs4 import BeautifulSoup

def fetch_race(race_id: str) -> pd.DataFrame:
  url = 'https://db.netkeiba.com/race/' + race_id
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

def fetch_horse(horse_id: str) -> pd.DataFrame:
  url = 'https://db.netkeiba.com/horse/' + horse_id
  df = pd.read_html(url, encoding='EUC-JP')
  df = df[4] if df.columns[0] == '受賞歴' else df[3]
  df = df.rename(columns=lambda x: x.replace(' ', ''))
  df.index = [horse_id] * len(df)
  return df