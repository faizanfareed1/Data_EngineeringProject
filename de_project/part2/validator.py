import pandas as pd
import logging

logger = logging.getLogger(__name__)


class EmployeeValidator:
    """Pre-processing validation and cleaning for raw employee records."""

    REQUIRED_COLUMNS = [
        "employee_id", "full_name", "email", "department", "gender",
        "country", "salary_eur", "hire_date", "years_experience",
        "performance_score", "contract_type", "weekly_hours",
    ]
    NUMERIC_COLUMNS = ["salary_eur", "years_experience", "performance_score", "weekly_hours"]

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Starting validation. Input shape: {df.shape}")
        df = self._check_required_columns(df)
        df = self._coerce_numeric(df)
        df = self._drop_missing_critical(df)
        df = self._drop_invalid_salary(df)
        df = self._drop_invalid_email(df)
        logger.info(f"Validation complete. Output shape: {df.shape}")
        return df

    def _check_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Input data is missing required columns: {missing}")
        return df

    def _coerce_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.NUMERIC_COLUMNS:
            if col in df.columns:
                before = df[col].isna().sum()
                df[col] = pd.to_numeric(df[col], errors="coerce")
                coerced = df[col].isna().sum() - before
                if coerced > 0:
                    logger.warning(f"Column '{col}': {coerced} non-numeric values set to NaN.")
        return df

    def _drop_missing_critical(self, df: pd.DataFrame) -> pd.DataFrame:
        critical = ["employee_id", "salary_eur"]
        before = len(df)
        df = df.dropna(subset=critical)
        dropped = before - len(df)
        if dropped:
            logger.warning(f"Dropped {dropped} rows missing critical fields {critical}.")
        return df

    def _drop_invalid_salary(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df[df["salary_eur"] > 0]
        dropped = before - len(df)
        if dropped:
            logger.warning(f"Dropped {dropped} rows with non-positive salary_eur.")
        return df

    def _drop_invalid_email(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        mask = df["email"].str.contains(r"^[^@]+@[^@]+\.[^@]+$", na=False, regex=True)
        df = df[mask]
        dropped = before - len(df)
        if dropped:
            logger.warning(f"Dropped {dropped} rows with invalid email addresses.")
        return df
