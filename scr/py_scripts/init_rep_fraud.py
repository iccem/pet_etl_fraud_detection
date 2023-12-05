import py_scripts.sql_scripts as scr
import py_scripts.utility as u


def _init_rep_fraud_tbl(cursor):
    """
    Create table if not exists rep_fraud.

    """
    script = scr.tbl_rep_fraud_init()
    cursor.execute(script)


def init_rep_fraud_tbl(con):
    """
    Drop table rep_fraud if exists and then
    create table rep_fraud.

    """
    cursor = con.cursor()
    try:
        u.delete_tbl(cursor, 'REP_FRAUD')
        _init_rep_fraud_tbl(cursor)
    except:
        print('Table\'s already existed.')
