import pandas as pd
import psycopg2
from psycopg2 import Error
from decouple import config

postgres_user, postgres_pass = config('postgres_user'), config('postgres_pass')
# print(postgres_user, postgres_pass)

def DBConnect(dbName=None):
    """

    Parameters
    ----------
    dbName :
        Default value = None)

    Returns
    -------

    """
    try:
        conn = psycopg2.connect(host='localhost',
                                user=postgres_user,
                                password=postgres_pass,
                                database=dbName,
                                port="5432")

        cur = conn.cursor()
        conn.autocommit = True
        print('Connection Established')

    except (Exception, Error) as error:
        print(error)
        conn, cur = None, None

    return conn, cur

#
# def emojiDB(dbName: str) -> None:
#     conn, cur = DBConnect(dbName)
#     dbQuery = f"ALTER DATABASE {dbName} CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;"
#     cur.execute(dbQuery)
#     conn.commit()

def createDB(dbName: str) -> None:
    """

    Parameters
    ----------
    dbName :
        str:
    dbName :
        str:
    dbName:str :


    Returns
    -------

    """
    try:
        postgres_db = config('postgres_user')
        # print(postgres_db)
        conn, cur = DBConnect(postgres_db)
        sql = f"CREATE DATABASE {dbName};"
        cur.execute(sql)
        # conn.commit()
        cur.close()

    except (Exception, Error) as e:
        print(e)

def createTables(dbName: str) -> None:
    """

    Parameters
    ----------
    dbName :
        str:
    dbName :
        str:
    dbName:str :


    Returns
    -------

    """
    conn, cur = DBConnect(dbName)
    sqlFile = '../data/table_schema.sql'
    fd = open(sqlFile, 'r')
    readSqlFile = fd.read()
    fd.close()

    sqlCommands = readSqlFile.split(';')

    for command in sqlCommands:
        try:
            res = cur.execute(command)
            response = "Table created sucessfuly"
        except Exception as ex:
            print("Command skipped: ", command)
            response = "Table Not Created"
            print(ex)
    conn.commit()
    cur.close()

    return print(response)

def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    """

    Parameters
    ----------
    df :
        pd.DataFrame:
    df :
        pd.DataFrame:
    df:pd.DataFrame :


    Returns
    -------

    """
    cols_2_drop = ['Unnamed: 0', 'timestamp']
    try:
        df = df.drop(columns=cols_2_drop, axis=1)
        df = df.fillna(0)
    except KeyError as e:
        print("Error:", e)

    return df


def insert_to_telecom_user_data(dbName: str, df: pd.DataFrame, table_name: str) -> None:
    """

    Parameters
    ----------
    dbName :
        str:
    df :
        pd.DataFrame:
    table_name :
        str:
    dbName :
        str:
    df :
        pd.DataFrame:
    table_name :
        str:
    dbName:str :

    df:pd.DataFrame :

    table_name:str :


    Returns
    -------

    """
    conn, cur = DBConnect(dbName)

    df = preprocess_df(df)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (bearer_id,Start_time,Start_ms,end_time,end_ms,duration_ms,IMSI,MSISDN_Number,IMEI,Last_Location_Name, Avg_RTT_DL_ms,
                                                Avg_RTT_UL_ms,TCP_DL_Retrans_Vol_Bytes,TCP_UL_Retrans_Vol_Bytes,Avg_Bearer_TP_DL_kbps,
                                                Avg_Bearer_TP_UL_kbps, Handset_Manufacturer, Handset_Type, Social_Media_DL_Bytes, Social_Media_UL_Bytes,
                                                social_media_data, google_data, email_data, youtube_data, netflix_data, gaming_data, total_dl_ul,
                                                RTT_DL_UL, TCP_DL_UL, TP_DL_UL)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        data = (row[0], row[1], row[2], row[3], (row[4]), (row[5]), row[6], row[7], row[8], row[9], row[10], row[11],
                row[14], row[15], row[12], (row[13]), (row[27]), row[28], row[31], row[32], row[47], row[48], row[49],
                row[50], row[51], row[52], (row[53]), (row[55]), row[56], row[57])

        try:
            # Execute the SQL command
            cur.execute(sqlQuery, data)
            # Commit your changes in the database
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    return

def db_execute_fetch(*args, many=False, tablename='', rdf=True, **kwargs) -> pd.DataFrame:
    """

    Parameters
    ----------
    *args :

    many :
         (Default value = False)
    tablename :
         (Default value = '')
    rdf :
         (Default value = True)
    **kwargs :


    Returns
    -------

    """
    connection, cursor1 = DBConnect(**kwargs)
    if many:
        cursor1.executemany(*args)
    else:
        cursor1.execute(*args)

    # get column names
    field_names = [i[0] for i in cursor1.description]

    # get column values
    res = cursor1.fetchall()

    # get row count and show info
    nrow = cursor1.rowcount
    if tablename:
        print(f"{nrow} recrods fetched from {tablename} table")

    cursor1.close()
    connection.close()

    # return result
    if rdf:
        return pd.DataFrame(res, columns=field_names)
    else:
        return res


if __name__ == "__main__":
    createDB(dbName='TelecomUserData')
    createTables(dbName='TelecomUserData')

    df = pd.read_csv('../data/processed_data.csv')
    print(df.head())

    insert_to_telecom_user_data(dbName='TelecomUserData', df=df, table_name='TelecomUserData')