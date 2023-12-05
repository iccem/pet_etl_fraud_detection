
def tbl_terminals_init():
    """
    Return the table TERMINALS creation script.

    """
    script = ''' 
        CREATE TABLE IF NOT EXISTS DWH_DIM_TERMINALS_HIST(
            terminal_id varchar(128),
            terminal_type varchar(128),
            terminal_city varchar(128),
            terminal_address varchar(128),
            deleted_flg integer default 0,
            effective_from datetime,
            effective_to datetime default (datetime('2999-12-31 23:59:59'))
        )
    '''
    return script


def tbl_transactions_init():
    """
    Return the table TRANSACTIONS creation script.

    """
    script = ''' 
        CREATE TABLE IF NOT EXISTS DWH_FACT_TRANSACTIONS(
            trans_id varchar(128),
            trans_date datetime,
            card_num varchar(128),
            open_type varchar(128),
            amt decimal,
            open_result varchar(128),
            terminal varchar(128),
            created_dt datetime,
            update_dt datetime
        )
    '''
    return script


def tbl_passport_blacklist_init():
    """
    Return the table PASSPORT_BLACKLIST creation script.

    """
    script = ''' 
        CREATE TABLE IF NOT EXISTS DWH_FACT_PASSPORT_BLACKLIST(
            passport_num varchar(128),
            entry_dt datetime,
            created_dt datetime,
            update_dt datetime
        )
    '''
    return script


def tbl_rep_fraud_init():
    """
    Return the table rep_fraud creation script.

    """
    script = ''' 
        CREATE TABLE IF NOT EXISTS REP_FRAUD(
            event_dt datetime,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt datetime default current_timestamp 
        )
    '''
    return script
