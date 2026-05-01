"""
Module for processing NYC Yellow Taxi records and creating derived features.
"""

# pylint: disable=trailing-newlines
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class TaxiProcessor:  # pylint: disable=too-few-public-methods
    """
    Processes the validated Yellow Taxi DataFrame.

    - Removes columns: VendorID, store_and_fwd_flag, RatecodeID
    - Adds derived columns as specified in the project brief
    """

    COLUMNS_TO_REMOVE = ["VendorID", "store_and_fwd_flag", "RatecodeID"]

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processes and enriches the validated taxi trip DataFrame."""
        logger.info("Starting processing step.")
        df = self._remove_columns(df)
        df = self._add_trip_duration(df)
        df = self._add_average_speed(df)
        df = self._add_pickup_year_month(df)
        df = self._add_revenue_per_mile(df)
        df = self._add_trip_distance_category(df)
        df = self._add_fare_category(df)
        df = self._add_trip_time_of_day(df)
        logger.info("Processing complete. Final shape: %s", df.shape)
        return df

    def _remove_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_to_drop = [c for c in self.COLUMNS_TO_REMOVE if c in df.columns]
        df = df.drop(columns=cols_to_drop)
        logger.info("Removed columns: %s", cols_to_drop)
        return df

    def _add_trip_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        df["trip_duration_minutes"] = (
            (
                df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
            ).dt.total_seconds()
            / 60
        ).round(2)
        logger.info("Added column: trip_duration_minutes")
        return df

    def _add_average_speed(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = df["trip_duration_minutes"] > 0
        df["average_speed_mph"] = None
        df.loc[mask, "average_speed_mph"] = (
            df.loc[mask, "trip_distance"] / (df.loc[mask, "trip_duration_minutes"] / 60)
        ).round(2)
        df["average_speed_mph"] = pd.to_numeric(df["average_speed_mph"])
        logger.info("Added column: average_speed_mph")
        return df

    def _add_pickup_year_month(self, df: pd.DataFrame) -> pd.DataFrame:
        df["pickup_year"] = df["tpep_pickup_datetime"].dt.year
        df["pickup_month"] = df["tpep_pickup_datetime"].dt.month
        logger.info("Added columns: pickup_year, pickup_month")
        return df

    def _add_revenue_per_mile(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = df["trip_distance"] > 0
        df["revenue_per_mile"] = None
        df.loc[mask, "revenue_per_mile"] = (
            df.loc[mask, "total_amount"] / df.loc[mask, "trip_distance"]
        ).round(2)
        df["revenue_per_mile"] = pd.to_numeric(df["revenue_per_mile"])
        logger.info("Added column: revenue_per_mile")
        return df

    def _add_trip_distance_category(self, df: pd.DataFrame) -> pd.DataFrame:
        def categorize(dist):
            if dist < 2:
                return "Short"
            if dist <= 10:
                return "Medium"
            return "Long"

        df["trip_distance_category"] = df["trip_distance"].apply(categorize)
        logger.info("Added column: trip_distance_category")
        return df

    def _add_fare_category(self, df: pd.DataFrame) -> pd.DataFrame:
        def categorize(fare):
            if fare < 20:
                return "Low"
            if fare <= 50:
                return "Medium"
            return "High"

        df["fare_category"] = df["fare_amount"].apply(categorize)
        logger.info("Added column: fare_category")
        return df

    def _add_trip_time_of_day(self, df: pd.DataFrame) -> pd.DataFrame:
        def categorize(hour):
            if 0 <= hour < 6:
                return "Night"
            if 6 <= hour < 12:
                return "Morning"
            if 12 <= hour < 18:
                return "Afternoon"
            return "Evening"

        df["trip_time_of_day"] = df["tpep_pickup_datetime"].dt.hour.apply(categorize)
        logger.info("Added column: trip_time_of_day")
        return df
