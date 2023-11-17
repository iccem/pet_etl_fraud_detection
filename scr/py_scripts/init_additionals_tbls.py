import py_scripts.sql_scripts as scr


def _init_transactions_tbl(cursor):
    script = scr.tbl_transactions_init()
    cursor.execute(script)


def _init_terminals_tbl(cursor):

    script = scr.tbl_terminals_init()
    cursor.execute(script)


def _init_passport_blacklist_tbl(cursor):
    script = scr.tbl_passport_blacklist_init()
    cursor.execute(script)


def init_additional_tbls(cursor) -> None:
    _init_transactions_tbl(cursor)
    _init_terminals_tbl(cursor)
    _init_passport_blacklist_tbl(cursor)
