"""特徴量生成モジュール（リーク防止込み）"""

import pandas as pd


class FeatureGenerator:
    """レースの特徴量を生成する"""

    def __init__(self, races: pd.DataFrame, race_profiles: pd.DataFrame, horses: pd.DataFrame):
        self.races = races
        self.race_profiles = race_profiles
        self.horses = horses

    def generate(self, race_id: str, cutoff_date: pd.Timestamp) -> pd.DataFrame:
        """
        指定レースの特徴量を生成する

        Args:
            race_id: 対象レースID
            cutoff_date: この日付より前のデータのみ使用（リーク防止）

        Returns:
            特徴量DataFrame（各馬1行）
        """
        raise NotImplementedError

    def compute_time_index(self, race_id: str, cutoff_date: pd.Timestamp) -> pd.Series:
        """タイム指数を計算する"""
        raise NotImplementedError

    def compute_horse_stats(self, horse_id: str, cutoff_date: pd.Timestamp) -> dict:
        """馬の過去成績統計を計算する"""
        raise NotImplementedError

    def compute_jockey_stats(self, jockey_id: str, cutoff_date: pd.Timestamp) -> dict:
        """騎手の過去成績統計を計算する"""
        raise NotImplementedError
