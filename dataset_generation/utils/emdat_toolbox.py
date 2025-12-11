"""emdat_toolbox.py

Helper functions for EM-DAT disaster data preprocessing

"""

import numpy as np
import pandas as pd
import ast
import calendar
import pandas as pd
import numpy as np
import calendar


def add_event_dates(emdat_df):
    """
    Add parsed datetime columns for event start and end dates.

    Parameters
    ----------
    emdat_df : pandas.DataFrame
        DataFrame with disaster event rows.

    Returns
    -------
    pandas.DataFrame
        DataFrame with 'Start Date' and 'End Date' datetime columns.
    """

    # If start day is NaN, assign it to the first date of the month and add a note to the flags column
    # If end day is NaN, assign it to the last day of the month and add a note to the flags column
    print("Filling missing start and end days...")
    emdat_df = fill_missing_start_end_days(emdat_df)

    print(f"Formatting start and end date and creating new columns...")
    emdat_df.loc[:, "Start Date"] = emdat_df.apply(
        lambda row: get_datetime(
            row["Start Year"], row["Start Month"], row["Start Day"]
        ),
        axis=1,
    )
    emdat_df.loc[:, "End Date"] = emdat_df.apply(
        lambda row: get_datetime(row["End Year"], row["End Month"], row["End Day"]),
        axis=1,
    )

    return emdat_df


def fill_missing_start_end_days(df):
    """
    Fills missing values in 'Start Day' and 'End Day' based on available month/year info,
    and appends notes to the 'data_processing_flags' column.

    - Sets 'Start Day' to 1 if it's NaN and both 'Start Month' and 'Start Year' are present.
    - Sets 'End Day' to the last day of the month if it's NaN and both 'End Month' and 'End Year' are present.
    - Adds corresponding messages to the 'data_processing_flags' column.

    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame with the following required columns:
        'Start Day', 'Start Month', 'Start Year',
        'End Day', 'End Month', 'End Year',
        and 'data_processing_flags'.

    Returns
    -------
    pd.DataFrame
        The modified DataFrame with filled day values and updated flags.
    """

    df = df.copy()

    # ---- Fix Start Day ----
    valid_start_info = (
        df["Start Day"].isna() & df["Start Month"].notna() & df["Start Year"].notna()
    )
    df.loc[valid_start_info, "Start Day"] = 1
    df.loc[valid_start_info, "data_processing_flags"] += "; Start day originally NaN"

    # ---- Fix End Day ----
    valid_end_info = (
        df["End Day"].isna() & df["End Month"].notna() & df["End Year"].notna()
    )

    # Compute last day of the month
    last_days = df.loc[valid_end_info].apply(
        lambda row: calendar.monthrange(int(row["End Year"]), int(row["End Month"]))[1],
        axis=1,
    )
    df.loc[valid_end_info, "End Day"] = last_days.values

    # Append End Day flag
    df.loc[valid_end_info, "data_processing_flags"] += "; End day originally NaN"

    return df


def expand_admin_units(row, static_columns):
    """
    Expands the "Admin Units" column into separate rows while retaining other static columns.

    Parameters
    ----------
    row : pd.Series
        A row from the input DataFrame.
    static_columns : list
        List of column names that should be retained in the expanded DataFrame.

    Returns
    -------
    list
        A list of dictionaries, each representing an expanded row.
    """
    try:
        # Read list string as python element
        admin_units = ast.literal_eval(row["Admin Units"])
        expanded_rows = []

        # Iterate over each administrative unit
        for unit in admin_units:
            # Create a new row preserving static columns
            expanded_row = {col: row[col] for col in static_columns}
            expanded_row.update(
                {
                    "Admin Units": admin_units,
                    "adm1_code": unit.get("adm1_code") or np.nan,
                    "adm1_name": (
                        np.nan
                        if unit.get("adm1_name") == "Administrative unit not available"
                        else unit.get("adm1_name") or np.nan
                    ),
                    "adm2_code": unit.get("adm2_code") or np.nan,
                    "adm2_name": (
                        np.nan
                        if unit.get("adm2_name") == "Administrative unit not available"
                        else unit.get("adm2_name") or np.nan
                    ),
                }
            )
            expanded_rows.append(expanded_row)

        return expanded_rows
    except (ValueError, SyntaxError, TypeError):
        # Return an empty list if there's an issue parsing the "Admin Units" column
        return []


def get_datetime(year, month, day):
    """
    Convert year, month, and day values into a pandas datetime object.
    Returns NaN if any input value is NaN.

    Parameters
    ----------
    year : float or int
        The year part of the date.
    month : float or int
        The month part of the date.
    day : float or int
        The day part of the date.

    Returns
    -------
    pd.Timestamp or np.nan
        A pandas datetime object corresponding to the given year, month, and day.
        If any input is NaN, or if the date cannot be constructed, returns 'np.nan'.
    """

    # Check for NaN values
    if any(np.isnan(val) for val in [year, month, day]):
        return np.nan

    # Convert year, month, and day from float to int for correct formatting
    year, month, day = map(int, (year, month, day))

    # Return the formatted datetime using pandas to_datetime function
    return pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")


def split_event_by_month(row):
    """
    Split a single disaster event row into multiple rows by month.

    Ensures that each resulting row contains:
    - Only the portion of the event occurring within a given month.
    - The appropriate 'Start Date' and 'End Date' clipped to that month.
    - A new 'mon-yr' column in 'MM-YYYY' format.
    - A new 'mon-yr-id' column to uniquely identify monthly slices.

    Parameters
    ----------
    row : pd.Series
        A single row from a disaster event DataFrame, containing 'Start Date' and 'End Date'.

    Returns
    -------
    pd.DataFrame
        A DataFrame with one row per month spanned by the event. All original columns are preserved.
    """
    start = row["Start Date"]
    end = row["End Date"]

    # Handle missing start or end dates
    if pd.isna(start) or pd.isna(end):
        row = row.copy()
        row["mon-yr"] = ""
        row["mon-yr-id"] = ""
        return pd.DataFrame([row])

    # Generate list of month starts from first of start month to end date
    months = pd.date_range(start.replace(day=1), end, freq="MS")

    rows = []
    for i, month_start in enumerate(months):
        # Last day of current month
        month_end = month_start + pd.offsets.MonthEnd(0)

        # Determine actual start and end dates for this chunk
        this_start = max(start, month_start)
        this_end = min(end, month_end)

        # Copy original row and update fields
        row_copy = row.copy()
        row_copy["Start Date"] = this_start
        row_copy["End Date"] = this_end
        row_copy["mon-yr"] = month_start.strftime("%m-%Y")
        row_copy["mon-yr-id"] = f"{month_start.strftime('%m')}-{row_copy['id']}"

        rows.append(row_copy)

    return pd.DataFrame(rows)
