class Query:
    """
    Base class for queries used in this project
    """

    def __init__(self):
        pass

    def _get_query(self, dts: dict[str, dict[str, str]]) -> str:
        """
        Get the formatted query, adjusted to current db materializaiton

        Parameters
        ----------
        dts : dict[str, dict[str, str]]
            Dictionary with data types for give columns of CTEs.
            E.g. {"o": {"o_orderkey": "extracted_list[1]::INT"}}

        Returns
        -------
        str
        """
        raise NotImplementedError()

    def columns_used(self):
        """
        Get the columns used in the query

        Returns
        -------
        list[str]
        """
        columns = []
        for clause, col_list in self.columns_used_with_position().items():
            if clause == "join":
                for field, join_fields in col_list.items():
                    columns.extend([field] * len(join_fields))
            elif clause == "self_join":
                for field, no_self_joins in col_list.items():
                    columns.extend([field] * 2 * no_self_joins)
            elif clause == "where":
                columns.extend(col_list)
            else:
                # continue
                columns.extend(col_list)
        return columns

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """

        Get the columns used in TPC-H Query 1 along with their position in the query
        (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of column names used in the SELECT clause.
            - 'where': list of column names used in the WHERE clause that are not joins.
            - 'group_by': list of column names used in the GROUP BY clause.
            - 'order_by': list of column names used in the ORDER BY clause.
            - 'join': list of column names used in a join operation (including WHERE).

        NOTE
        Must be implemented by each subclass to list all cols in select/where/join/etc.
        """
        raise NotImplementedError()

    def get_cte_setups(self) -> dict[str, list[str]]:
        """
        Get the CTE names and columns for this particular query
        """
        raise NotImplementedError()

    def _get_field_types(self, fields: list[tuple[str, dict, bool]]) -> dict[str, str]:

        used_columns = self.columns_used()

        if fields is None:
            return {col: None for col in used_columns}

        data_types = dict()

        for col, access_query, materialized in fields:
            if col in used_columns:
                if materialized:
                    data_types[col] = None
                else:
                    data_types[col] = access_query["type"]

        return data_types

    def _json(self, tbl: str, col: str, dts: dict[str, dict[str, str]]):
        """
        Extract the column

        Parameters
        ----------
        tbl : str
            The table alias to extract from
        col : str
            The column name to extract
        dts : dict[str, dict[str, str]]
            Dictionary with data types for give columns of CTEs.
            E.g. {"o": {"o_orderkey": "extracted_list[1]::INT"}}

        Returns
        -------
        str
            The column extracted from json, or directly from respective CTE

        """
        col_alias = dts[tbl][col]

        return f"{tbl}.{col_alias}"

    def _get_cte(self, cte_name: str, cte_columns: list[str], field_types: dict[str, str]) -> tuple[str, dict[str, str]]:

        cte_stmt = f"{cte_name} AS (SELECT "

        col_accesses = dict()
        cte_list = []

        for cte_column in cte_columns:
            field_type = field_types[cte_column]
            if field_type is None:
                col_accesses[cte_column] = cte_column
                cte_stmt += f"{cte_column},"
            else:
                cte_list.append(cte_column)
                # JSON arrays are 1-indexed -> use len(cte_list) after appending
                col_accesses[cte_column] = f"extracted_list[{len(cte_list)}]::{field_type}"

        if len(cte_list) == 0:
            # Remove the last comma if all are materialized
            cte_stmt = cte_stmt[:-1]
        else:
            # Prepare the json_extract statement
            cte_stmt += "json_extract_string(raw_json, ["
            for cte_list_item in cte_list:
                cte_stmt += f"'{cte_list_item}', "
            # Remove the last comma
            cte_stmt = cte_stmt[:-2]
            cte_stmt += "]) AS extracted_list "

        # End statement
        cte_stmt += " FROM test_table)"

        return cte_stmt, col_accesses

    def get_query(
            self,
            fields: list[tuple[str, dict, bool]]
    ):
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax

        Parameters
        ----------
        fields : list[tuple[str, dict, bool]]
            The field definitions, with the indexes in the tuple corresponding to
            - column name
            - JSON extraction string
            - whether the field is materialized or not
        """
        cte_setups = self.get_cte_setups()
        field_types = self._get_field_types(fields=fields)

        # query_str = "WITH "
        # field_accesses = dict()

        # # Update with the individual CTEs
        # for cte_name, cte_columns in cte_setups.items():
        #     cte_stmt, cte_field_accesses = self._get_cte(cte_name=cte_name,
        #                                                  cte_columns=cte_columns, field_types=field_types)
        #     query_str += cte_stmt + ", "
        #     field_accesses[cte_name] = cte_field_accesses

        # # Remove trailing comma
        # query_str = query_str[:-2]

        # # Generer with {table_name} AS ()
        # query_str += " " + self._get_query(dts=field_accesses)

        # return query_str
        # 1) flatten all columns across all aliases into one list (preserving order)

        seen = set()
        all_columns = []
        for cols in cte_setups.values():
            for col in cols:
                if col not in seen:
                    seen.add(col)
                    all_columns.append(col)

        # 2) emit exactly one CTE called "extracted"
        cte_stmt, global_accesses = self._get_cte(
            cte_name="extracted",
            cte_columns=all_columns,
            field_types=field_types
        )

        # 3) re-split the single accesses map back into per-alias maps
        field_accesses = {}
        for alias, cols in cte_setups.items():
            field_accesses[alias] = {
                col: global_accesses[col]
                for col in cols
            }

        # 4) stitch together the final SQL
        return f"WITH {cte_stmt} " + self._get_query(dts=field_accesses)
