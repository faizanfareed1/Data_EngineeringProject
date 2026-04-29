import pandas as pd
import logging

logger = logging.getLogger(__name__)


class TaxiBackupValidator:
    """
    Post-processing validation: confirms all derived columns are present
    and contain sensible values after the Processor has run.
    """

    REQUIRED_DERIVED_COLUMNS = [
        "trip_duration_minutes",
        "average_speed_mph",
        "pickup_year",
        "pickup_month",
        "revenue_per_mile",
        "trip_distance_category",
        "fare_category",
        "trip_time_of_day",
    ]

    REMOVED_COLUMNS = ["VendorID", "store_and_fwd_flag", "RatecodeID"]

    VALID_DISTANCE_CATEGORIES = {"Short", "Medium", "Long"}
    VALID_FARE_CATEGORIES = {"Low", "Medium", "High"}
    VALID_TIME_OF_DAY = {"Night", "Morning", "Afternoon", "Evening"}

    def validate(self, df: pd.DataFrame) -> bool:
        logger.info("Starting backup (post-processing) validation.")
        passed = True

        passed &= self._check_derived_columns_exist(df)
        passed &= self._check_removed_columns_gone(df)
        passed &= self._check_trip_duration(df)
        passed &= self._check_average_speed(df)
        passed &= self._check_pickup_year_month(df)
        passed &= self._check_revenue_per_mile(df)
        passed &= self._check_categorical_values(df)

        if passed:
            logger.info("Backup validation PASSED — all derived columns are valid.")
        else:
            logger.error("Backup validation FAILED — check warnings above.")

        return passed

    def _check_derived_columns_exist(self, df: pd.DataFrame) -> bool:
        missing = [c for c in self.REQUIRED_DERIVED_COLUMNS if c not in df.columns]
        if missing:
            logger.error(f"Missing derived columns after processing: {missing}")
            return False
        logger.info("All derived columns are present.")
        return True

    def _check_removed_columns_gone(self, df: pd.DataFrame) -> bool:
        still_present = [c for c in self.REMOVED_COLUMNS if c in df.columns]
        if still_present:
            logger.warning(f"Columns that should have been removed are still present: {still_present}")
            return False
        logger.info("All columns to remove have been successfully removed.")
        return True

    def _check_trip_duration(self, df: pd.DataFrame) -> bool:
        if "trip_duration_minutes" not in df.columns:
            return False
        neg = (df["trip_duration_minutes"] < 0).sum()
        if neg:
            logger.warning(f"trip_duration_minutes has {neg} negative values.")
            return False
        return True

    def _check_average_speed(self, df: pd.DataFrame) -> bool:
        if "average_speed_mph" not in df.columns:
            return False
        # Allow nulls (where duration was 0), but no negatives
        neg = (df["average_speed_mph"].dropna() < 0).sum()
        if neg:
            logger.warning(f"average_speed_mph has {neg} negative values.")
            return False
        return True

    def _check_pickup_year_month(self, df: pd.DataFrame) -> bool:
        ok = True
        if "pickup_year" in df.columns:
            if df["pickup_year"].isnull().any():
                logger.warning("pickup_year contains null values.")
                ok = False
        if "pickup_month" in df.columns:
            invalid = ~df["pickup_month"].between(1, 12)
            if invalid.any():
                logger.warning(f"pickup_month has {invalid.sum()} values outside 1–12.")
                ok = False
        return ok

    def _check_revenue_per_mile(self, df: pd.DataFrame) -> bool:
        if "revenue_per_mile" not in df.columns:
            return False
        neg = (df["revenue_per_mile"].dropna() < 0).sum()
        if neg:
            logger.warning(f"revenue_per_mile has {neg} negative values.")
            return False
        return True

    def _check_categorical_values(self, df: pd.DataFrame) -> bool:
        ok = True
        checks = [
            ("trip_distance_category", self.VALID_DISTANCE_CATEGORIES),
            ("fare_category", self.VALID_FARE_CATEGORIES),
            ("trip_time_of_day", self.VALID_TIME_OF_DAY),
        ]
        for col, valid_set in checks:
            if col not in df.columns:
                continue
            invalid = ~df[col].isin(valid_set)
            if invalid.any():
                logger.warning(f"'{col}' has {invalid.sum()} values outside {valid_set}.")
                ok = False
        return ok
