"""
Module for validating NYC Yellow Taxi Trip Records DataFrame.
"""

# pylint: disable=trailing-newlines
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class TaxiValidator:  # pylint: disable=too-few-public-methods
    """
    Validates the Yellow Taxi Trip Records DataFrame.

    Mandatory columns must be non-null and pass type/range checks.
    Non-mandatory columns are checked if present but nulls are allowed.
    Rows failing mandatory validation are dropped and logged.
    """

    MANDATORY_COLUMNS = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "total_amount",
    ]

    NON_MANDATORY_COLUMNS = [
        "tip_amount",
        "tolls_amount",
        "extra",
        "Airport_fee",  # note: capital A in dataset
        "congestion_surcharge",
        "cbd_congestion_fee",
        "store_and_fwd_flag",
        "RatecodeID",
    ]

    VALID_PAYMENT_TYPES = {1, 2, 3, 4, 5, 6}  # per TLC data dictionary
    VALID_RATECODEID = {1, 2, 3, 4, 5, 6}
    VALID_STORE_FWD = {"Y", "N"}
    MAX_PASSENGER_COUNT = 9
    MAX_TRIP_DISTANCE = 500  # miles — anything above is likely erroneous
    MAX_FARE = 1_000  # dollars
    MAX_SPEED_MPH = 200  # anything faster than 200 mph is erroneous for a taxi

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validates input taxi trip records according to TLC specifications."""
        original_count = len(df)
        logger.info("Starting validation on %s rows.", original_count)

        if df.empty:
            logger.warning("Input DataFrame is empty — skipping all validation checks.")
            return df

        df = self._check_mandatory_columns_exist(df)
        df = self._drop_mandatory_nulls(df)
        df = self._validate_datetime_columns(df)
        df = self._validate_pickup_before_dropoff(df)
        df = self._validate_passenger_count(df)
        df = self._validate_trip_distance(df)
        df = self._validate_location_ids(df)
        df = self._validate_payment_type(df)
        df = self._validate_fare_amount(df)
        df = self._validate_total_amount(df)
        self._validate_non_mandatory_columns(df)

        removed = original_count - len(df)
        logger.info(
            "Validation complete. Removed %s invalid rows. %s rows remaining.",
            removed,
            len(df),
        )
        return df

    # ------------------------------------------------------------------ #
    #  Mandatory checks
    # ------------------------------------------------------------------ #

    def _check_mandatory_columns_exist(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.MANDATORY_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing mandatory columns: {missing}")
        logger.info("All mandatory columns present.")
        return df

    def _drop_mandatory_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.dropna(subset=self.MANDATORY_COLUMNS)
        dropped = before - len(df)
        if dropped:
            logger.warning("Dropped %s rows with nulls in mandatory columns.", dropped)
        return df

    def _validate_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                logger.warning(
                    "Column '%s' is not datetime — attempting conversion.", col
                )
                df[col] = pd.to_datetime(df[col], errors="coerce")
        before = len(df)
        df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
        dropped = before - len(df)
        if dropped:
            logger.warning("Dropped %s rows with unparseable datetime values.", dropped)
        return df

    def _validate_pickup_before_dropoff(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = df["tpep_pickup_datetime"] >= df["tpep_dropoff_datetime"]
        bad = mask.sum()
        if bad:
            logger.warning("Dropped %s rows where pickup >= dropoff datetime.", bad)
            df = df[~mask]
        return df

    def _validate_passenger_count(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = (df["passenger_count"] < 1) | (
            df["passenger_count"] > self.MAX_PASSENGER_COUNT
        )
        bad = mask.sum()
        if bad:
            logger.warning(
                "Dropped %s rows with invalid passenger_count (must be 1-%s).",
                bad,
                self.MAX_PASSENGER_COUNT,
            )
            df = df[~mask]
        return df

    def _validate_trip_distance(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = (df["trip_distance"] < 0) | (
            df["trip_distance"] > self.MAX_TRIP_DISTANCE
        )
        bad = mask.sum()
        if bad:
            logger.warning(
                "Dropped %s rows with invalid trip_distance (must be 0-%s).",
                bad,
                self.MAX_TRIP_DISTANCE,
            )
            df = df[~mask]
        return df

    def _validate_location_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["PULocationID", "DOLocationID"]:
            mask = (df[col] < 1) | (df[col] > 265)  # TLC zones: 1–265
            bad = mask.sum()
            if bad:
                logger.warning(
                    "Dropped %s rows with invalid %s (must be 1–265).", bad, col
                )
                df = df[~mask]
        return df

    def _validate_payment_type(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = ~df["payment_type"].isin(self.VALID_PAYMENT_TYPES)
        bad = mask.sum()
        if bad:
            logger.warning(
                "Dropped %s rows with invalid payment_type (valid: %s).",
                bad,
                self.VALID_PAYMENT_TYPES,
            )
            df = df[~mask]
        return df

    def _validate_fare_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = (df["fare_amount"] < 0) | (df["fare_amount"] > self.MAX_FARE)
        bad = mask.sum()
        if bad:
            logger.warning(
                "Dropped %s rows with invalid fare_amount (must be 0-%s).",
                bad,
                self.MAX_FARE,
            )
            df = df[~mask]
        return df

    def _validate_total_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = (df["total_amount"] < 0) | (df["total_amount"] > self.MAX_FARE)
        bad = mask.sum()
        if bad:
            logger.warning(
                "Dropped %s rows with invalid total_amount (must be 0-%s).",
                bad,
                self.MAX_FARE,
            )
            df = df[~mask]
        return df

    # ------------------------------------------------------------------ #
    #  Non-mandatory checks (log warnings only, no rows dropped)
    # ------------------------------------------------------------------ #

    def _validate_non_mandatory_columns(self, df: pd.DataFrame) -> None:
        for col in self.NON_MANDATORY_COLUMNS:
            if col not in df.columns:
                logger.info("Non-mandatory column '%s' not present in dataset.", col)
                continue

            null_count = df[col].isnull().sum()
            if null_count:
                logger.info(
                    "Non-mandatory column '%s' has %s null values (allowed).",
                    col,
                    null_count,
                )

            # Type-specific soft checks
            if col == "store_and_fwd_flag":
                bad = df[col].dropna()
                bad = bad[~bad.isin(self.VALID_STORE_FWD)]
                if len(bad):
                    logger.warning(
                        "'%s' has %s unexpected values (expected Y/N).", col, len(bad)
                    )

            elif col == "RatecodeID":
                bad = df[col].dropna()
                bad = bad[~bad.isin(self.VALID_RATECODEID)]
                if len(bad):
                    logger.warning(
                        "'%s' has %s values outside valid range %s.",
                        col,
                        len(bad),
                        self.VALID_RATECODEID,
                    )

            elif col in (
                "tip_amount",
                "tolls_amount",
                "extra",
                "Airport_fee",
                "congestion_surcharge",
                "cbd_congestion_fee",
            ):
                neg = (df[col].dropna() < 0).sum()
                if neg:
                    logger.warning("'%s' has %s negative values.", col, neg)
