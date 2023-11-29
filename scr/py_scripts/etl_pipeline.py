import py_scripts.utility as ut
import py_scripts.transactions as ld
import py_scripts.terminals as lt
import py_scripts.init_additionals_tbls as adt
import py_scripts.passport_blacklist as pbl
import py_scripts.init_rep_fraud as r
import py_scripts.rep_fraud as rf


def create_etl_pipeline(con, date_report: str):
    """
    Creates additional tables and load
    data for terminals, transactions
    and passport blacklist reports.

    """
    path = 'D:/Icc/PycharmProjects/pet_etl_fraud_detection/pet_etl_fraud_detection/data/'
    cursor = con.cursor()
    adt.init_additional_tbls(cursor)
    date_report = date_report.replace('-', '')
    path_terminals = path + 'terminals_02032021.xlsx'
    # path_terminals = path + 'terminals_' + date_report + '.xlsx'
    try:
        lt.load_terminals_report(con, path_terminals)
        ut.upload(path_terminals)
    except (FileNotFoundError, IOError):
        print("A new report on terminals was not found.")
        ut.delete_tbl(cursor, 'STG_TERMINALS')
        ut.delete_view(cursor, 'STG_V_TERMINALS')

    path_transactions = r'transactions_' + date_report + '.txt'
    try:
        ld.load_transactions_report(con, path_transactions)
        ut.upload(path_transactions)
    except (FileNotFoundError, IOError):
        print("A new report on transactions was not found.")
        ut.delete_tbl(cursor, 'STG_TRANSACTIONS')

    path_pass = r'passport_blacklist_' + date_report + '.xlsx'
    try:
        pbl.load_passport_blacklist_report(con, path_pass)
        ut.upload(path_pass)
    except (FileNotFoundError, IOError):
        print("A new report on passports blacklist was not found.")
        ut.delete_tbl(cursor, 'STG_PASSPORT_BLACKLIST')


def get_fraud_report(con, date_report: str) -> None:
    """
    Creates fraud report.

    """
    r.init_rep_fraud_tbl(con)

    rf.create_fraud_report(con, date_report)
