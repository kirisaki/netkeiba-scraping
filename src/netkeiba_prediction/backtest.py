"""バックテストモジュール"""

from datetime import date
import pandas as pd

from .features import FeatureGenerator
from .models import RankPredictor
from .evaluate import Evaluator


class Backtester:
    """時系列バックテストを実行する"""

    def __init__(
        self,
        races: pd.DataFrame,
        race_profiles: pd.DataFrame,
        horses: pd.DataFrame,
        payouts: pd.DataFrame,
    ):
        self.races = races
        self.race_profiles = race_profiles
        self.horses = horses
        self.payouts = payouts

    def run(
        self,
        train_start: date,
        train_end: date,
        test_start: date,
        test_end: date,
    ) -> dict:
        """
        バックテストを実行する

        Args:
            train_start: 学習期間開始
            train_end: 学習期間終了
            test_start: テスト期間開始
            test_end: テスト期間終了

        Returns:
            評価結果のdict
        """
        raise NotImplementedError

    def walk_forward(
        self,
        start: date,
        end: date,
        train_window: int,
        test_window: int,
    ) -> list[dict]:
        """
        ウォークフォワード分析を実行する

        Args:
            start: 開始日
            end: 終了日
            train_window: 学習期間（日数）
            test_window: テスト期間（日数）

        Returns:
            各期間の評価結果リスト
        """
        raise NotImplementedError
