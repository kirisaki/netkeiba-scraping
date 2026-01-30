"""予測モデルモジュール"""

import pandas as pd


class RankPredictor:
    """順位予測モデル"""

    def __init__(self):
        self.model = None

    def train(self, features: pd.DataFrame, targets: pd.Series):
        """
        モデルを学習する

        Args:
            features: 特徴量DataFrame
            targets: 目的変数（着順やタイム）
        """
        raise NotImplementedError

    def predict(self, features: pd.DataFrame) -> pd.Series:
        """
        順位を予測する

        Args:
            features: 特徴量DataFrame

        Returns:
            予測値（タイム指数など）
        """
        raise NotImplementedError

    def predict_rank(self, features: pd.DataFrame) -> pd.Series:
        """
        予測値を順位に変換する

        Args:
            features: 特徴量DataFrame

        Returns:
            予測順位
        """
        raise NotImplementedError

    def save(self, path: str):
        """モデルを保存する"""
        raise NotImplementedError

    def load(self, path: str):
        """モデルを読み込む"""
        raise NotImplementedError
