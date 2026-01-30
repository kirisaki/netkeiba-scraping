"""評価モジュール"""

import pandas as pd


class Evaluator:
    """予測結果を評価する"""

    @staticmethod
    def accuracy_at_1(predicted_rank: pd.Series, actual_rank: pd.Series) -> float:
        """1着的中率を計算する"""
        raise NotImplementedError

    @staticmethod
    def accuracy_at_3(predicted_rank: pd.Series, actual_rank: pd.Series) -> float:
        """上位3頭が3着以内に入る率を計算する"""
        raise NotImplementedError

    @staticmethod
    def rank_correlation(predicted_rank: pd.Series, actual_rank: pd.Series) -> float:
        """順位相関（Spearman）を計算する"""
        raise NotImplementedError

    @staticmethod
    def summary(predicted_rank: pd.Series, actual_rank: pd.Series) -> dict:
        """評価指標のサマリーを返す"""
        raise NotImplementedError
