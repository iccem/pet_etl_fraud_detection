import py_scripts.utility as ut
import pandas as pd


def load_passport_blacklist_report(con, file_path):
    doc_timestamp = str(ut.get_date(file_path))

    load_tbl_tmp(con, file_path)
    create_new_rows(con, doc_timestamp)
    update_rows(con, doc_timestamp)


def load_tbl_tmp(con, file_path):
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS STG_PASSPORT_BLACKLIST(
            entry_dt date,
            passport_num varchar(128)
        )
    ''')

    df = pd.read_excel(file_path, engine='openpyxl')
    df.rename(columns = {
        'date' : 'entry_dt',
        'passport' : 'passport_num'
    }, inplace = True)
    df.to_sql('STG_PASSPORT_BLACKLIST', con, if_exists='replace', index=False)
    con.commit()


def create_new_rows(con, doc_timestamp):
    doc_timestamp = doc_timestamp + ' 00:00:00'
    cursor = con.cursor()

    cursor.execute('''
        CREATE TABLE if not exists STG_NEW_ROWS as
            SELECT
                entry_dt,
                passport_num
            FROM STG_PASSPORT_BLACKLIST
            WHERE entry_dt = ?
    ''', [doc_timestamp])


def update_rows(con, doc_timestamp):
    cursor = con.cursor()
    cursor.execute('''
        INSERT INTO DWH_FACT_PASSPORT_BLACKLIST(
            passport_num,
            entry_dt,
            created_dt
        ) SELECT
            passport_num,
            entry_dt,
            ?
        FROM STG_NEW_ROWS
    ''', [doc_timestamp])
    
    con.commit()

    ut.delete_tbl(cursor, 'STG_NEW_ROWS')
    ut.delete_tbl(cursor, 'STG_PASSPORT_BLACKLIST')
