from datetime import datetime
from datetime import date
from datetime import timedelta
import py_scripts.utility as u


# Совершение операции при просроченном или заблокированном паспорте.
def get_report_by_not_valid_passport(con, readable_report_dt):
    cursor = con.cursor()

    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS STG_NOT_VALID_PASSPORT as
                SELECT
                    client_id,
                    last_name || ' ' || first_name || ' ' || patronymic as fio,
                    phone,
                    passport_num,
                    passport_valid_to
                FROM
                    DWH_DIM_CLIENTS
                WHERE
                    passport_valid_to <= ? 
                    or passport_num in (
                        SELECT 
                            passport_num
                        FROM
                            DWH_FACT_PASSPORT_BLACKLIST
                    );
        ''', [readable_report_dt])

        cursor.execute('''
        INSERT INTO REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        )
            SELECT 
                t4.trans_date as event_dt,
                t1.passport_num as passport,
                t1.fio,
                t1.phone,
                'not valid passport' as event_type,
                ? as report_dt
            FROM STG_NOT_VALID_PASSPORT t1
            INNER JOIN DWH_DIM_ACCOUNTS t2
            ON t1.client_id = t2.client
            INNER JOIN  DWH_DIM_CARDS t3
            ON t2.account = t3.account
            INNER JOIN  DWH_FACT_TRANSACTIONS t4
            ON t3.card_num = t4.card_num
            GROUP BY passport
        ''', [date.today()])

        con.commit()
        cursor.execute('DROP TABLE IF EXISTS STG_NOT_VALID_PASSPORT')
    except:
        cursor.execute('DROP TABLE IF EXISTS STG_NOT_VALID_PASSPORT')

# Совершение операции при недействующем договоре.
def get_report_by_not_valid_account(con, readable_report_dt):
    cursor = con.cursor()

    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS STG_NOT_VALID_ACCOUNT as
                SELECT
                    t1.client_id,
                    last_name || ' ' || first_name || ' ' || patronymic as fio,
                    phone,
                    passport_num,
                    t2.account,
                    t2.valid_to
                FROM
                    DWH_DIM_CLIENTS t1 
                    INNER JOIN DWH_DIM_ACCOUNTS t2
                    ON t1.client_id = t2.client
                WHERE
                    t2.valid_to <= ?
        ''', [readable_report_dt])


        cursor.execute('''
            INSERT INTO REP_FRAUD (
                event_dt,
                passport,
                fio,
                phone,
                event_type,
                report_dt
            )
                SELECT 
                    t1.trans_date as event_dt,
                    t4.passport_num as passport,
                    t4.fio,
                    t4.phone,
                    'not valid account' as event_type,
                    ? as report_dt
                FROM STG_NOT_VALID_ACCOUNT t4
                INNER JOIN DWH_DIM_ACCOUNTS t3
                ON t4.client_id = t3.client
                INNER JOIN  DWH_DIM_CARDS t2
                ON t3.account = t2.account
                INNER JOIN  DWH_FACT_TRANSACTIONS t1
                ON t1.card_num = t2.card_num
                GROUP BY passport
        ''', [date.today()])

        con.commit()
        cursor.execute('DROP TABLE IF EXISTS STG_NOT_VALID_ACCOUNT')
    except:
        cursor.execute('DROP TABLE IF EXISTS STG_NOT_VALID_ACCOUNT')


# Совершение операций в разных городах в течение одного часа.
def get_report_by_multy_cities_per_hour(con, readable_report_dt):
    cursor = con.cursor()

    try:
        cursor.execute('''
            create table STG_TRANSACTION_BY_CITY AS
                select
                    T4.card_num,
                    trans_date,
                    terminal_city
                from DWH_FACT_TRANSACTIONS t3
                inner join 
                (select 
                    card_num, 
                    terminal_city,
                    count(distinct terminal_city) as c
                from DWH_FACT_TRANSACTIONS t1 
                left join DWH_DIM_TERMINALS_HIST t2
                on t1.terminal = t2.terminal_id
                group by card_num
                having c > 1) t4
                on t3.card_num = t4.card_num;
        ''')
        # print('='*50)
        # print('STG_TRANSACTION_BY_CITY')
        # u.showTable(cursor, 'STG_TRANSACTION_BY_CITY')

        cursor.execute('''
            create table STG_COMPARE_CITIES AS
                select 
                    card_num,
                    terminal_city,
                    trans_date,
                    lag(terminal_city) over(order by card_num) as prev_city,
                    lag(trans_date) over(order by card_num) as prev_time
                    --, (select time(min(trans_date),'+59 minutes'))  as period_59_mins
                from
                (select
                    trans_id,
                    trans_date,
                    card_num,
                    terminal_city
                from DWH_FACT_TRANSACTIONS t1
                left join DWH_DIM_TERMINALS_HIST t2
                on t1.terminal = t2.terminal_id) STG_TRANSACTION_BY_CITY
                group by card_num, trans_date, terminal_city
                having card_num in (
                                    SELECT 
                                        card_num
                                    FROM STG_TRANSACTION_BY_CITY
                                    group by card_num, terminal_city
                                    ) ;
                                    --and terminal_city <> prev_city;
        ''')
        # print('='*50)
        # print('STG_COMPARE_CITIES')
        # u.showTable(cursor, 'STG_COMPARE_CITIES')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS STG_NOT_VALID_TRANSACTION_BY_CITY as
                select
                    card_num,
                    min(trans_date) as trans_date
                from (
                select 
                    card_num,
                    terminal_city,
                    trans_date,
                    prev_city,
                    prev_time,
                    (
                    Select Cast ((
                    JulianDay(trans_date) - JulianDay(prev_time)
                ) * 24 * 60 As Integer)) as interval
                from STG_COMPARE_CITIES
                where
                    terminal_city <> prev_city
                    and interval between 0 and 60 )
                group by card_num
                having trans_date <= ?
        ''', [readable_report_dt])
        # print('='*50)
        # print('STG_NOT_VALID_TRANSACTION_BY_CITY')
        # u.showTable(cursor, 'STG_NOT_VALID_TRANSACTION_BY_CITY')

        cursor.execute('''
            create table STG_DWH_DIM_CLIENTS_info AS
                select
                    t1.card_num,
                    t2.account,
                    t2.client,
                    t3.passport_num,
                    t3.phone,
                    last_name || ' ' || first_name || ' ' || patronymic as fio
                from
                DWH_DIM_CARDS t1
                inner join DWH_DIM_ACCOUNTS t2
                on t1.account = t2.account
                inner join DWH_DIM_CLIENTS t3
                on t2.client = t3.client_id
        ''')
        # print('='*50)
        # print('STG_DWH_DIM_CLIENTS_info')
        # u.showTable(cursor, 'STG_DWH_DIM_CLIENTS_info')


        cursor.execute('''
            INSERT INTO REP_FRAUD (
                event_dt,
                passport,
                fio,
                phone,
                event_type,
                report_dt
            )
                SELECT 
                    t1.trans_date as event_dt,
                    t2.passport_num as passport,
                    t2.fio,
                    t2.phone,
                    'not valid by city' as event_type,
                    ? as report_dt
                FROM STG_NOT_VALID_TRANSACTION_BY_CITY t1
                INNER JOIN STG_DWH_DIM_CLIENTS_info t2
                ON t1.card_num = t2.card_num
        ''', [date.today()])
        con.commit()
        cursor.execute('DROP TABLE IF EXISTS STG_TRANSACTION_BY_CITY')
        cursor.execute('DROP TABLE IF EXISTS STG_COMPARE_CITIES')
        cursor.execute('DROP TABLE IF EXISTS STG_NOT_VALID_TRANSACTION_BY_CITY')
        cursor.execute('DROP TABLE IF EXISTS STG_DWH_DIM_CLIENTS_info')
    except:
        cursor.execute('DROP TABLE IF EXISTS STG_TRANSACTION_BY_CITY')
        cursor.execute('DROP TABLE IF EXISTS STG_COMPARE_CITIES')
        cursor.execute('DROP TABLE IF EXISTS STG_NOT_VALID_TRANSACTION_BY_CITY')
        cursor.execute('DROP TABLE IF EXISTS STG_DWH_DIM_CLIENTS_info')
        

    # try:
    #     print('='*50)
    #     print('REP_FRAUD')
    #     u.showTable(cursor, 'REP_FRAUD')
    # except:
    #     print('NO REPORT')



def get_report_by_fraud_operations(con, readable_report_dt):
    cursor = con.cursor()

    try:
        cursor.execute('''
            create table STG_fraud_TRANSACTIONs AS
                select
                    trans_date,
                    card_num,
                    (
                        Select Cast ((
                        JulianDay(prev_trans_date_1) - JulianDay(prev_trans_date_4)
                    ) * 24 * 60 As Integer)) as interval
                from (
                        select
                        card_num,
                        open_type,
                        open_result,
                        trans_date,
                        amt,
                        lag(open_result, 1) over(partition by card_num order by trans_date) as prev_result_1,
                        lag(open_result, 2) over(partition by card_num order by trans_date) as prev_result_2,
                        lag(open_result, 3) over(partition by card_num order by trans_date) as prev_result_3,
                        lag(open_result, 4) over(partition by card_num order by trans_date) as prev_result_4,
                        lag(amt, 1) over(partition by card_num order by trans_date) as prev_amt_1,
                        lag(amt, 2) over(partition by card_num order by trans_date) as prev_amt_2,
                        lag(amt, 3) over(partition by card_num order by trans_date) as prev_amt_3,
                        lag(amt, 4) over(partition by card_num order by trans_date) as prev_amt_4,
                        lag(trans_date, 1) over(partition by card_num order by trans_date) as prev_trans_date_1,
                        lag(trans_date, 2) over(partition by card_num order by trans_date) as prev_trans_date_2,
                        lag(trans_date, 3) over(partition by card_num order by trans_date) as prev_trans_date_3,
                        lag(trans_date, 4) over(partition by card_num order by trans_date) as prev_trans_date_4
                        from DWH_FACT_TRANSACTIONS
                        group by card_num, trans_date
                        )
                        where 
                            prev_result_1 = 'SUCCESS' 
                        and prev_result_2 = 'REJECT'
                        and prev_result_3 = 'REJECT'
                        and prev_result_4 = 'REJECT'
                        and (
                            prev_amt_1 < prev_amt_2 
                        and prev_amt_2 < prev_amt_3
                        and prev_amt_3 < prev_amt_4
                        )
                        and interval <= 20;
        ''')

        cursor.execute('''
            create table STG_DWH_DIM_CLIENTS_info AS
                select
                    t1.card_num,
                    t2.account,
                    t2.client,
                    t3.passport_num,
                    t3.phone,
                    last_name || ' ' || first_name || ' ' || patronymic as fio
                from
                DWH_DIM_CARDS t1
                inner join DWH_DIM_ACCOUNTS t2
                on t1.account = t2.account
                inner join DWH_DIM_CLIENTS t3
                on t2.client = t3.client_id
        ''')

        cursor.execute('''
            INSERT INTO REP_FRAUD (
                event_dt,
                passport,
                fio,
                phone,
                event_type,
                report_dt
            )
                SELECT 
                    t1.trans_date as event_dt,
                    t2.passport_num as passport,
                    t2.fio,
                    t2.phone,
                    'fraud operation' as event_type,
                    ? as report_dt
                FROM STG_fraud_TRANSACTIONs t1
                INNER JOIN STG_DWH_DIM_CLIENTS_info t2
                ON t1.card_num = t2.card_num
        ''', [date.today()])
        con.commit()
        cursor.execute('DROP TABLE IF EXISTS STG_fraud_TRANSACTIONs')
        cursor.execute('DROP TABLE IF EXISTS STG_DWH_DIM_CLIENTS_info')
    except:
        cursor.execute('DROP TABLE IF EXISTS STG_fraud_TRANSACTIONs')
        cursor.execute('DROP TABLE IF EXISTS STG_DWH_DIM_CLIENTS_info')
        cursor.execute('DROP TABLE IF EXISTS REP_FRAUD')


def if_fraud(con, report_dt):
    readable_report_dt = u.get_readable_date(report_dt)
    datetime_object = datetime.strptime(readable_report_dt, '%Y-%m-%d').date()
    shift_day = datetime_object + timedelta(days=1)
    readable_report_dt = str(shift_day)


    get_report_by_not_valid_passport(con, readable_report_dt)
    get_report_by_not_valid_account(con, readable_report_dt)
    get_report_by_multy_cities_per_hour(con, readable_report_dt)
    get_report_by_fraud_operations(con, readable_report_dt)
