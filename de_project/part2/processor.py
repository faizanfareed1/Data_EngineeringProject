"""
Module for processing and enriching Employee Records DataFrame.
"""

# pylint: disable=trailing-newlines
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class EmployeeProcessor:  # pylint: disable=too-few-public-methods
    """
    Processes validated employee HR records.

    Normalizes messy fields and adds 4 derived columns:
    - salary_band
    - seniority_level
    - gender_normalized
    - years_at_company
    """

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processes and applies derived metrics to employee records."""
        logger.info("Starting processing step.")
        if "employee_id" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["employee_id"], keep="first")
            dropped = before - len(df)
            if dropped:
                logger.info("Dropped %d duplicate employee records.", dropped)
        df = self._normalize_gender(df)
        df = self._normalize_contract_type(df)
        df = self._add_salary_band(df)
        df = self._add_seniority_level(df)
        df = self._add_years_at_company(df)
        df = self._add_is_senior(df)
        logger.info("Processing complete. Final shape: %s", df.shape)
        return df

    def _normalize_gender(self, df: pd.DataFrame) -> pd.DataFrame:
        if "gender" not in df.columns:
            return df

        def normalize(val):
            if pd.isna(val):
                return "Unknown"
            v = str(val).strip().upper()
            if v in ("M", "MALE"):
                return "Male"
            if v in ("F", "FEMALE"):
                return "Female"
            return "Unknown"

        df["gender_normalized"] = df["gender"].apply(normalize)
        logger.info("Added column: gender_normalized")
        return df

    def _normalize_contract_type(self, df: pd.DataFrame) -> pd.DataFrame:
        if "contract_type" not in df.columns:
            return df

        def normalize(val):
            if pd.isna(val):
                return "Unknown"
            v = str(val).strip().lower()
            if "full" in v:
                return "Full-time"
            if "part" in v:
                return "Part-time"
            if "free" in v:
                return "Freelance"
            return "Unknown"

        df["contract_type"] = df["contract_type"].apply(normalize)
        logger.info("Normalized column: contract_type")
        return df

    def _add_salary_band(self, df: pd.DataFrame) -> pd.DataFrame:
        def band(salary):
            if salary < 2000:
                return "Entry"
            if salary < 4000:
                return "Mid"
            if salary < 7000:
                return "Senior"
            return "Executive"

        df["salary_band"] = df["salary_eur"].apply(band)
        logger.info("Added column: salary_band")
        return df

    def _add_seniority_level(self, df: pd.DataFrame) -> pd.DataFrame:
        if "years_experience" not in df.columns:
            return df

        def seniority(yrs):
            if pd.isna(yrs) or yrs < 0:
                return "Unknown"
            if yrs < 2:
                return "Junior"
            if yrs < 5:
                return "Mid-level"
            if yrs < 10:
                return "Senior"
            return "Principal"

        df["seniority_level"] = df["years_experience"].apply(seniority)
        logger.info("Added column: seniority_level")
        return df

    def _add_years_at_company(self, df: pd.DataFrame) -> pd.DataFrame:
        if "hire_date" not in df.columns:
            return df

        today = pd.Timestamp.today()
        df["hire_date"] = pd.to_datetime(df["hire_date"], errors="coerce")
        df["years_at_company"] = ((today - df["hire_date"]).dt.days / 365.25).round(1)
        logger.info("Added column: years_at_company")
        return df

    def _add_is_senior(self, df: pd.DataFrame) -> pd.DataFrame:
        """Boolean flag: True if salary_band is Senior or Executive."""
        df["is_senior"] = df["salary_band"].isin({"Senior", "Executive"})
        logger.info("Added column: is_senior")
        return df
