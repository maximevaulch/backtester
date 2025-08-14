# In Core/strategy_base.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any



class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.

    This class defines the common interface that all strategy modules must implement.
    It ensures that each strategy provides the necessary metadata and a method
    for generating trading signals.
    """

    @property
    @abstractmethod
    def STRATEGY_TIMEFRAME(self) -> str:
        """The primary timeframe for the strategy (e.g., '15min', '1h')."""
        pass

    @property
    @abstractmethod
    def SESSION_TYPE(self) -> str:
        """Session type ('required', 'optional', 'none')."""
        pass

    @property
    @abstractmethod
    def AVAILABLE_FILTERS(self) -> List[str]:
        """A list of names for the available filters the strategy provides."""
        pass

    @abstractmethod
    def generate_conditions(self, df: pd.DataFrame, strategy_params: Dict[str, Any] = {}) -> pd.DataFrame:
        """
        The core method to generate trading conditions and signals.

        Args:
            df: A pandas DataFrame containing the market data.
            strategy_params: A dictionary of parameters to customize the strategy's behavior.

        Returns:
            A pandas DataFrame with the same index as `df`, containing boolean columns
            for 'base_pattern_cond', 'is_bullish', 'is_bearish', and potentially
            'session_cond' and other filter conditions. It must also include
            'entry_price', 'sl_price_long', and 'sl_price_short'.
        """
        pass
