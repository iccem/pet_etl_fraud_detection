import pandas as pd
import py_scripts.utility as ut


def load_transactions_report(con, file_path):
    doc_timestamp = str(ut.get_date(file_path))

    load_transactions_tmp(con, file_path)
    update_transactions(con, doc_timestamp)


def load_transactions_tmp(con, file_path):
    cursor = con.cursor()
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS STG_TRANSACTIONS(
                trans_id varchar(128),
                trans_date datetime,
                card_num varchar(128),
                open_type varchar(128),
                amt decimal,
                open_result varchar(128),
                terminal varchar(128)
        )
    ''')

    df = pd.read_csv(file_path, sep=';', encoding='utf-8')
    df.rename(columns = {
        'transaction_id' : 'trans_id', 
        'transaction_date' : 'trans_date', 
        'amount' : 'amt', 
        'card_num' : 'card_num', 
        'oper_type' : 'open_type', 
        'oper_result' : 'open_result', 
        'terminal' : 'terminal', 
        }, inplace = True)
    df = df[['trans_id', 'trans_date', 'card_num', 'open_type', 'amt', 'open_result', 'terminal']]
    num_rows = df.shape[0]
    for row in range(0, num_rows):
        cursor.execute('''
            INSERT INTO STG_TRANSACTIONS(
                trans_id,
                trans_date,
                card_num,
                open_type,
                amt,
                open_result,
                terminal
            ) VALUES 
                (?, ?, ?, ?, ?, ?, ?)''',
                [str(df['trans_id'][row]), 
                df['trans_date'][row], 
                df['card_num'][row], 
                df['open_type'][row], 
                df['amt'][row], 
                df['open_result'][row], 
                df['terminal'][row]])
        con.commit()


def update_transactions(con, doc_timestamp):
    cursor = con.cursor()
    cursor.execute('''
        INSERT INTO DWH_FACT_TRANSACTIONS(
            trans_id,
            trans_date,
            card_num,
            open_type,
            amt,
            open_result,
            terminal,
            created_dt
        ) SELECT
            trans_id,
            trans_date,
            card_num,
            open_type,
            amt,
            open_result,
            terminal,
            ?
        FROM STG_TRANSACTIONS
    ''', [doc_timestamp])
    
    con.commit()

    ut.delete_tbl(cursor, 'STG_TRANSACTIONS')
