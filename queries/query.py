class Query:
    """
    Base class for queries used in this project
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted query, adjusted to current db materializaiton

        Returns
        -------
        str
        """

    def get_used_cols(self):
        """
        Get the columns used in the query

        Returns
        -------
        list[str]
        """

    def _json(self, tbl: str, col: str, dt: str):
        """
        Extract the column 

        Parameters
        ----------
        tbl : str
            The table alias to extract from
        col : str
            The column name to extract
        dt : str | None
            The date type of the column to extract

        Returns
        -------
        str
            The column extracted from json. If `dt` is None, there is no json extraction

        """
        if dt is None:
            return f"{tbl}.{col}"

        return f"CAST({tbl}.raw_json->>'{col}' AS {dt})"
