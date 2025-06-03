"""
Load all the data with SQL and pass them for the further
analysis.

transform.py only defines „how”, and the outer code decides
„when” and „for which inputs.”
"""


class PivotOneIndicator:
    """
    Its sole job is: given a “wide” table (one row per
    country, one column per year), produce a “long” table
    (rows = country × year × value).
    It should not run automatically—only when you explicitly
    call its method.
    """
