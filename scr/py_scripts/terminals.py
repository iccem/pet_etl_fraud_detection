import pandas as pd
import py_scripts.utility as ut


def load_terminals_report(con, file_path):
    doc_timestamp = str(ut.get_date(file_path))
    
    load_terminals_tmp(con, file_path)
    create_new_rows(con)
    create_deleted_rows(con)
    create_changed_rows(con)
    update_terminals(con, doc_timestamp)


def load_terminals_tmp(con, file_path):
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS STG_TERMINALS(
            terminal_id varchar(128),
            terminal_type varchar(128),
            terminal_city varchar(128),
            terminal_address varchar(128)
        )
    ''')

    cursor.execute('''
        CREATE VIEW IF NOT EXISTS STG_V_TERMINALS as
            SELECT
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address
            FROM DWH_DIM_TERMINALS_HIST
            WHERE current_timestamp between effective_from and effective_to
            AND deleted_flg = 0        
    ''')

    df = pd.read_excel(file_path, engine='openpyxl')
    df.to_sql('STG_TERMINALS', con, if_exists='replace', index=False)
    con.commit()


def create_new_rows(con):
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS STG_NEW_ROWS as
            SELECT
                t1.terminal_id,
                t1.terminal_type,
                t1.terminal_city,
                t1.terminal_address
            FROM STG_TERMINALS t1
            LEFT JOIN STG_V_TERMINALS t2
            ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id is null
    ''')


def create_deleted_rows(con):
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS STG_DELETED_ROWS as
            SELECT
                t1.terminal_id,
                t1.terminal_type,
                t1.terminal_city,
                t1.terminal_address
            FROM STG_V_TERMINALS t1
            LEFT JOIN STG_TERMINALS t2
            ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id is null
    ''')


def create_changed_rows(con):
    cursor = con.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS STG_CHANGED_ROWS as
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address
        FROM STG_TERMINALS t1
        INNER JOIN STG_V_TERMINALS t2
        ON t1.terminal_id = t2.terminal_id
        AND (     t1.terminal_id         <> t2.terminal_id
               or t1.terminal_type       <> t2.terminal_type
               or t1.terminal_city       <> t2.terminal_city
               or t1.terminal_address    <> t2.terminal_address
        )
    ''')


def update_terminals(con, doc_timestamp):
    cursor = con.cursor()
    cursor.execute('''
        INSERT INTO DWH_DIM_TERMINALS_HIST(
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
        ) SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            ?
        FROM STG_NEW_ROWS
    ''', [doc_timestamp])

    cursor.execute(''' 
        UPDATE DWH_DIM_TERMINALS_HIST
        SET effective_to = ?
        WHERE terminal_id IN (select terminal_id FROM STG_CHANGED_ROWS)
        AND effective_to = datetime('2999-12-31 23:59:59')
	''', [doc_timestamp])

    cursor.execute(''' 
        INSERT INTO DWH_DIM_TERMINALS_HIST(
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
        ) SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            ?
        FROM STG_CHANGED_ROWS
    ''', [doc_timestamp])

    cursor.execute(''' 
		UPDATE DWH_DIM_TERMINALS_HIST
        SET effective_to = ?
        WHERE terminal_id IN (select terminal_id FROM STG_DELETED_ROWS)
        AND effective_to = datetime('2999-12-31 23:59:59')
	''', [doc_timestamp])

    cursor.execute(''' 
        INSERT INTO DWH_DIM_TERMINALS_HIST(
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from,
            deleted_flg
        ) SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            ?,
            1
        FROM STG_DELETED_ROWS
    ''', [doc_timestamp])
    con.commit()

    ut.delete_tbl(cursor, 'STG_TERMINALS')
    ut.delete_view(cursor, 'STG_V_TERMINALS')
    ut.delete_tbl(cursor, 'STG_NEW_ROWS')
    ut.delete_tbl(cursor, 'STG_DELETED_ROWS')
    ut.delete_tbl(cursor, 'STG_CHANGED_ROWS')
