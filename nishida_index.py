import pandas as pd
import pickle

if __name__ == '__main__':
  with open('./output/data.pickle', 'rb') as f:
    (races, horses, _, _) = pickle.load(f)
  print(races)
  print(horses)