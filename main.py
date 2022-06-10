import oracledb
import configparser
from datetime import datetime

date = datetime.now().strftime("%d/%b/%y")
TAXPAYEPIN = 'LU74995000M'
IDNUMBER = '7499500'
SERIALNUMBER = 7499510000
CHASSIS_NUMBER = 'LUN74995000NBI00'
ENTRYNUMBER = '2022NRB4023000'
PLATENUMBER = 'JAATFS54H'

def insert_nrb_id(environment):
    config = configparser.RawConfigParser()
    config.read('config.properties')
    user = config.get('details', 'user')
    password = config.get('details', 'password')
    dsn = config.get('details', 'dsn') + environment

    sqlText = "Insert into NTSABOMO" + environment + ".NRB_ID (ID_NUMBER,SERIAL_NUMBER,DATE_OF_ISSUE,FULL_NAMES,GENDER,DATE_OF_BIRTH,DISTRICT_OF_BIRTH,FATHER_NAMES,MOTHER_NAMES,ADDRESS)  values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)"
    try:
        with oracledb.connect(user=user,
                              password=password,
                              dsn=dsn) as connection:
            with connection.cursor() as cursorObj:
                cursorObj.execute(sqlText,
                                  (IDNUMBER, SERIALNUMBER, date, 'LUNGILE PEARL MOTSWENI', 'F', 19900109, 'NAKURU',
                                   'LONDI', 'NOMSA', 'PO BOX 12, NAIROBI'))
                print(connection.commit())
    except oracledb.Error as Error:
        print(Error)


def insert_kra_individual_pin(environment):
    config = configparser.RawConfigParser()
    config.read('config.properties')
    user = config.get('details', 'user')
    password = config.get('details', 'password')
    dsn = config.get('details', 'dsn') + environment

    sqlText = "Insert into NTSABOMO" + environment + ".KRA_PIN_INDIVIDUAL (FIRSTNAME,TAXPAYERPIN,RTELNO,MPOBOX,STARTUPDATE,IDTYPE,IDNUMBER,POCODE,MIDDLENAME,MTOWN,LASTNAME) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)"
    try:
        with oracledb.connect(user=user,
                              password=password,
                              dsn=dsn) as connection:
            with connection.cursor() as cursorObj:
                cursorObj.execute(sqlText, (
                    'LUNGILE', TAXPAYEPIN, '072202004', 'PO BOX 12, NAIROBI', 20220806, 'ID', IDNUMBER, 1520, 'PEARL',
                    'NAIROBI', 'MOTSWENI'))
                connection.commit()
    except oracledb.Error as Error:
        print(Error)


def insert_kra_simba(environment):
    config = configparser.RawConfigParser()
    config.read('config.properties')
    user = config.get('details', 'user')
    password = config.get('details', 'password')
    dsn = config.get('details', 'dsn') + environment

    sqlText = "Insert into NTSABOMO" + environment + ".KRA_SIMBA (ANDCL,CHASSIS_NUMBER,BURDCL,NUMDCL,IDMAKE,IDBODYTYPE,STATUS,PREVREGNUMBER,ENGNUMBER,IDNUMBER,IDMODEL,GAZNOTNUMBER,GAZNOTDATE,LOTNUMBER,AMNTAUCPAID,DRISIDE,AUCTYPE,TARWEIGHT,IDCOUNORIGIN,ENGINE_CAPACITY,YEAR_MANUFACTURE,RELEASEDATE,AMOUNT,PIN,IDVEHICLESOURCE,CPC,COUNTRYOFDESTINATION,BORDERPOINT,PLACEOFDISCHARGE,CODE602,CODE740,CODE601,CODE640,CODE722,CODE744,ENTRYNUMBER,REGISTRATION_STATUS_CD) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21,:22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36 ,:37)"
    try:
        with oracledb.connect(user=user,
                              password=password,
                              dsn=dsn) as connection:
            with connection.cursor() as cursorObj:
                cursorObj.execute(sqlText, (
                    '2022', CHASSIS_NUMBER, 'NBI', 875844, '18', 2, '0', None, '131742', PLATENUMBER, '0', 0, None,
                    None, 0,
                    '0', 0, 364, 'JP', 2500, 2010, date, None, TAXPAYEPIN, 9, 'C410', None, None, 'NAIROBI EXPORTS',
                    2000, 5100, 0, 0, 0, 0, ENTRYNUMBER, '0'))
                connection.commit()
    except oracledb.Error as Error:
        print(Error)


def connection_status(environment):
    return environment


class TestDataManager:
    env = "INT"
    # insert_nrb_id(env)
    # print(insert_kra_individual_pin(env))
    insert_kra_simba(env)
