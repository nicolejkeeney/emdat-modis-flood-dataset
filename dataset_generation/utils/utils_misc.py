"""
utils_misc.py

Miscellaneous, general utils functions used across scripts

"""

import os


def map_years_to_gpw_intervals():
    """
    Map years between 2000 and 2024 to their corresponding GPW 5-year interval.

    Returns
    -------
    dict
        Dictionary where keys are years (2000–2024) and values are the
        associated GPW reference year (e.g., 2000–2004 → 2000).
    """
    return {year: year - (year % 5) for year in range(2000, 2025)}


def summarize_flags(flags_df, verbose=True):
    """
    Summarize the number and percentage of unique 'mon-yr-adm1-id' and 'id' associated with each flag.

    Parameters
    ----------
    flags_df : pd.DataFrame
        DataFrame containing at least the columns 'mon-yr-adm1-id', 'id', and 'flags'.
        The 'flags' column should contain semicolon-separated flag numbers as strings.
    verbose : bool, optional
        If True, prints summary for each flag. Default is True.

    Returns
    -------
    dict
        Dictionary with flag numbers as keys and dictionaries containing counts & percent of total.
    """
    # Ensure 'flags' column is string and handle NaN
    flags_expanded = flags_df[["mon-yr-adm1-id", "id", "flags"]].copy()
    flags_expanded["flags"] = flags_expanded["flags"].fillna("")

    # Split and strip whitespace from each flag
    flags_expanded["flags_list"] = (
        flags_expanded["flags"]
        .str.split(";")
        .apply(lambda x: [f.strip() for f in x if f.strip()])
    )

    # Get all unique flags
    all_flags = list(
        {flag for sublist in flags_expanded["flags_list"] for flag in sublist}
    )

    # Sort numerically if possible
    try:
        all_flags.sort(key=lambda x: int(x))
    except ValueError:
        all_flags.sort()  # fallback to string sort if non-numeric flags exist

    # Total unique counts for percentage calculation
    total_mon_yr_adm1 = flags_expanded["mon-yr-adm1-id"].nunique()
    total_id = flags_expanded["id"].nunique()

    # Build results dictionary
    results = {}
    for flag in all_flags:
        subset = flags_expanded[flags_expanded["flags_list"].apply(lambda x: flag in x)]
        count_mon_yr_adm1 = subset["mon-yr-adm1-id"].nunique()
        count_id = subset["id"].nunique()

        pct_mon_yr_adm1 = (
            (count_mon_yr_adm1 / total_mon_yr_adm1) * 100 if total_mon_yr_adm1 else 0
        )
        pct_id = (count_id / total_id) * 100 if total_id else 0

        results[flag] = {
            "mon_yr_adm1_count": count_mon_yr_adm1,
            "mon_yr_adm1_pct": round(pct_mon_yr_adm1, 2),
            "id_count": count_id,
            "id_pct": round(pct_id, 2),
        }

        # Print to console
        if verbose:
            print(
                f"Flag {flag}: {count_mon_yr_adm1} mon-yr-adm1-id ({pct_mon_yr_adm1:.2f}%), "
                f"{count_id} id ({pct_id:.2f}%)"
            )

    return results


def check_dir_exists(dir):
    """
    Raise an error if a directory does not exist.

    Parameters
    ----------
    dir : str
        Path to the directory.

    Raises
    ------
    NotADirectoryError
        If the directory does not exist.
    """
    if not os.path.isdir(dir):
        raise NotADirectoryError(f"Directory does not exist: {dir}")


def check_file_exists(filepath):
    """
    Raise an error if a file does not exist.

    Parameters
    ----------
    filepath : str
        Path to the file.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    """
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"File does not exist: {filepath}")
