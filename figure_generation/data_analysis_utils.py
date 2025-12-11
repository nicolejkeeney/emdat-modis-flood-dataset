import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import numpy as np

# Figure settings
FIG_DPI = 500
TITLE_FONTSIZE = 18
TICK_FONTSIZE = 13
LABEL_FONTSIZE = 14
plt.rcParams["font.family"] = "Georgia"


def filter_by_flags(df, flags, exclude=False):
    """Filter rows in a DataFrame by flags.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing a column named 'flags' with semicolon-separated values.
    flags : list of str
        List of flag values to search for.
    exclude : bool, default False
        If True, return rows that do NOT contain any of the specified flags.

    Returns
    -------
    pd.DataFrame
        Subset of rows containing (or not containing) at least one of the specified flags.
    """
    # Error handling
    if type(flags) != list:
        flags = [flags]

    # Fill NaN with empty string
    df = df.copy()
    df["flags"] = df["flags"].fillna("")

    # Build regex pattern using non-capturing groups
    pattern = r"|".join([rf"(?:^|;\s*){flag}(?:$|;\s*)" for flag in flags])

    # Filter rows
    mask = df["flags"].str.contains(pattern, regex=True)

    if exclude:
        mask = ~mask  # invert mask to get rows without the flags

    return df[mask]


def plot_scatter_with_regression(
    data,
    x_col,
    y_col,
    title=None,
    xlabel=None,
    ylabel=None,
    figsize=(10, 6),
    one_to_one=True,
    xlim=None,
    ylim=None,
    tick_interval=None,
    save_path=None,
):
    """
    Create scatter plot with regression line and statistics.

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing the data
    x_col : str
        Column name for x-axis variable
    y_col : str
        Column name for y-axis variable
    title : str, optional
        Plot title. If None, auto-generated from column names
    xlabel : str, optional
        X-axis label. If None, uses x_col
    ylabel : str, optional
        Y-axis label. If None, uses y_col
    figsize : tuple, optional
        Figure size (width, height)
    one_to_one : bool, optional
        Add one-to-one line? Default True
    xlim : tuple, optional
        Manually set x-axis limits (min, max)
    ylim : tuple, optional
        Manually set y-axis limits (min, max)
    tick_interval : float, optional
        Interval for both x and y ticks; respects manual limits
    save_path : str, optional
        Path to save the figure. If None, figure is not saved.

    Returns
    -------
    dict
        Dictionary with regression statistics: r2, correlation, p_value, slope, intercept,
        xlim, ylim
    """
    plt.figure(figsize=figsize)

    # Compute regression
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        data[x_col], data[y_col]
    )

    sns.scatterplot(data=data, x=x_col, y=y_col, alpha=0.8)
    sns.regplot(
        data=data,
        x=x_col,
        y=y_col,
        scatter=False,
        color="red",
        label=f"y = {slope:.2f}x + {intercept:.2f}",
    )

    # Apply manual axis limits if provided
    if xlim is not None:
        plt.xlim(xlim)
    if ylim is not None:
        plt.ylim(ylim)

    # Get the final limits to use for tick placement
    xlim_final = plt.gca().get_xlim()
    ylim_final = plt.gca().get_ylim()

    # Set ticks at regular intervals if requested
    if tick_interval is not None:
        plt.xticks(
            np.arange(xlim_final[0], xlim_final[1] + tick_interval, tick_interval)
        )
        plt.yticks(
            np.arange(ylim_final[0], ylim_final[1] + tick_interval, tick_interval)
        )

    # Set labels and title
    plt.xlabel(xlabel or x_col, fontsize=LABEL_FONTSIZE)
    plt.ylabel(ylabel or y_col, fontsize=LABEL_FONTSIZE)
    plt.xticks(fontsize=TICK_FONTSIZE)
    plt.yticks(fontsize=TICK_FONTSIZE)
    title = title or f"{y_col} vs {x_col}"
    plt.title(title, fontsize=TITLE_FONTSIZE, y=1.02)
    plt.tight_layout(pad=1)

    # Add statistics
    p_thresh = 0.001
    p_value_text = f"P = {p_value:.2f}" if (p_value > p_thresh) else f"P < {p_thresh}"
    stats_text = f"R² = {r_value**2:.2f}, Slope = {slope:.2f}, {p_value_text}"
    plt.text(
        0.96,
        0.97,
        stats_text,
        ha="right",
        va="top",
        transform=plt.gca().transAxes,
        fontsize=LABEL_FONTSIZE,
        bbox=dict(
            facecolor="lightgrey",
            edgecolor="none",
            alpha=0.75,
            boxstyle="round,pad=0.3",
        ),
    )

    # One-to-one line
    if one_to_one:
        ax = plt.gca()
        line_min, line_max = ax.get_xlim()
        plt.plot(
            [line_min, line_max],
            [line_min, line_max],
            "k--",
            alpha=0.7,
            linewidth=1.5,
            label="y = x",
        )

    plt.legend(fontsize=LABEL_FONTSIZE)

    if save_path is not None:
        plt.savefig(save_path, dpi=FIG_DPI, bbox_inches="tight")
        print(f"Saved figure to {save_path}")

    return {
        "r2": r_value**2,
        "correlation": r_value,
        "p_value": p_value,
        "slope": slope,
        "intercept": intercept,
        "xlim": plt.gca().get_xlim(),
        "ylim": plt.gca().get_ylim(),
    }
