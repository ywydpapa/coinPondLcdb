import os
import pyupbit
import time
from datetime import datetime
import pymysql
import random
import pandas as pd
import dotenv
import sqlite3

dotenv.load_dotenv()
hostenv = os.getenv("host")
userenv = os.getenv("user")
passwordenv = os.getenv("password")
dbenv = os.getenv("db")
charsetenv = os.getenv("charset")

db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
serviceNo = 250204


def initsqlite():
    connlc = sqlite3.connect("cpondLoc.db")
    cursor = connlc.cursor()
    cursor.execute(f"DELETE FROM traceUser;")  # 테이블의 모든 데이터 삭제
    cursor.execute(f"DELETE FROM traceSets;")  # 테이블의 모든 데이터 삭제
    cursor.execute(f"DELETE FROM traceSetup;")  # 테이블의 모든 데이터 삭제
    connlc.commit()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS traceSetup (
	setupNo INTEGER NOT NULL, userNo INTEGER,initAsset REAL, bidInterval INTEGER, bidRate REAL, askRate REAL, bidCoin TEXT, activeYN TEXT, custKey TEXT, serverNo INTEGER, holdYN TEXT, holdNo INTEGER, doubleYN TEXT, limitYN TEXT, limitAmt REAL, slot INTEGER, regDate TEXT, attrib TEXT DEFAULT ('100001000010000'),
	CONSTRAINT traceSetup_pk PRIMARY KEY (setupNo))
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS traceSets (
	setNo INTEGER,setTitle TEXT,setInterval INTEGER,setp0 REAL,setp1 REAL,step2 REAL,step3 REAL,step4 REAL,step5 REAL,step6 REAL,step7 REAL,step8 REAL,step9 REAL,
	inter0 REAL,inter1 REAL,inter2 REAL,inter3 REAL,inter4 REAL,inter5 REAL,inter6 REAL,inter7 REAL,inter8 REAL,inter9 REAL,
	bid0 REAL,bid1 REAL,bid2 REAL,bid3 REAL,bid4 REAL,bid5 REAL,bid6 REAL,bid7 REAL,bid8 REAL,bid9 REAL,
	max0 REAL,	max1 REAL,	max2 REAL,	max3 REAL,	max4 REAL,	max5 REAL,	max6 REAL,	max7 REAL,	max8 REAL,	max9 REAL,
	net0 TEXT DEFAULT ('N'), net1 TEXT DEFAULT ('N'), net2 TEXT DEFAULT ('N'), net3 TEXT DEFAULT ('N'), net4 TEXT DEFAULT ('N'), net5 TEXT DEFAULT ('N'), net6 TEXT DEFAULT ('N'), net7 TEXT DEFAULT ('N'), net8 TEXT DEFAULT ('N'), net9 TEXT DEFAULT ('N'),
	useYN TEXT, regDate TEXT, modDate TEXT, attrib TEXT DEFAULT ('100001000010000'),
	CONSTRAINT traceSets_pk PRIMARY KEY (setNo))
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS traceUser (
	userNo INTEGER NOT NULL, userId TEXT, userName TEXT, userPasswd TEXT , apiKey1 TEXT, apiKey2 TEXT, setupKey TEXT, lastLogin TEXT, serverNo INTEGER, userRole TEXT, tradeCnt INTEGER, memo TEXT, server TEXT, attrib TEXT,
	CONSTRAINT traceUSer_pk PRIMARY KEY (userNo))
    ''')
    connlc.commit()
    connlc.close()

def loadMariatoLite(svrno):
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur13 = db.cursor()
    connlc = sqlite3.connect("cpondLoc.db")
    cursor = connlc.cursor()
    print('------------------------------------------------DB Sync------------------------------------------------')
    try:
        sql = "select * from traceSets where attrib not like %s"
        cur13.execute(sql, '%XXXUP')
        sets = list(cur13.fetchall())
        for set in sets:
            cursor.execute('''INSERT OR REPLACE INTO traceSets (setNo, setTitle,setInterval,setp0,setp1,step2,step3,step4,step5,step6,step7,step8,step9,
            inter0,inter1,inter2,inter3,inter4,inter5,inter6,inter7,inter8,inter9,bid0,bid1,bid2,bid3,bid4,bid5,bid6,bid7,bid8,bid9,
            max0,max1,max2,max3,max4,max5,max6,max7,max8,max9,net0,net1,net2,net3,net4,net5,net6,net7,net8,net9,
            useYN,regDate,modDate,attrib) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', set)
        connlc.commit()
        sql1 = "select * from traceSetup where serverNo = %s and attrib not like %s"
        cur13.execute(sql1, (svrno,'%XXXUP'))
        setups = list(cur13.fetchall())
        if setups != None:
            for setup in setups:
                cursor.execute(''' INSERT OR REPLACE INTO traceSetup (setupNo,userNo,initAsset,bidInterval,bidRate,askRate,bidCoin,activeYN,custKey,serverNo,holdYN,holdNo,doubleYN,limitYN,limitAmt,slot,regDate,attrib)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', setup)
            connlc.commit()
        sql2 = "select * from traceUser where serverNo = %s and attrib not like %s"
        cur13.execute(sql2, (svrno, '%XXXUP'))
        users = list(cur13.fetchall())
        if users != None:
            for user in users:
                cursor.execute(''' INSERT OR REPLACE INTO traceUser (userNo,userId,userName,userPasswd,apiKey1,apiKey2,setupKey,lastLogin,serverNo,userRole,tradeCnt,memo,server,attrib)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', user)
            connlc.commit()
        connlc.close()
    except Exception as e:
        print('접속오류', e)
    finally:
        cur13.close()
        db.close()


def getmsetup(uno):
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur13 = db.cursor()
    try:
        sql = "select * from tradingSetup where userNo=%s and attrib not like %s"
        cur13.execute(sql, (uno, '%XXXUP'))
        data = list(cur13.fetchall())
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cur13.close()
        db.close()


def getmsetup_tr(uno):
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur13 = db.cursor()
    try:
        sql = "select * from traceSetup where userNo=%s and attrib not like %s"
        cur13.execute(sql, (uno, '%XXXUP'))
        data = list(cur13.fetchall())
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cur13.close()
        db.close()


def getmsetup_trloc(uno):
    connlc = sqlite3.connect("cpondLoc.db")
    cursor = connlc.cursor()
    try:
        cursor.execute('''select * from traceSetup where userNo= ? and attrib not like ?''',(uno[0], "%XXXUP"))
        data = list(cursor.fetchall())
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cursor.close()


def getseton():
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur15 = db.cursor()
    data = []
    print("GetKey !!")
    try:
        sql = "SELECT userNo from tradingSetup where attrib not like %s"
        cur15.execute(sql, '%XXXUP')
        data = cur15.fetchall()
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cur15.close()
        db.close()


def getsetonsvr(svrNo):
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur16 = db.cursor()
    data = []
    try:
        sql = "SELECT distinct userNo from tradingSetup where attrib not like %s and serverNo=%s"
        cur16.execute(sql, ('%XXXUP', svrNo))
        data = cur16.fetchall()
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cur16.close()
        db.close()


def getsetonsvr_tr(svrNo):
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur16 = db.cursor()
    data = []
    try:
        sql = "SELECT distinct userNo from traceSetup where attrib not like %s and serverNo=%s"
        cur16.execute(sql, ('%XXXUP', svrNo))
        data = cur16.fetchall()
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cur16.close()
        db.close()


def getsetonsvr_trLoc(svrNo):
    connlc = sqlite3.connect("cpondLoc.db")
    cursor = connlc.cursor()
    try:
        cursor.execute('''select distinct userNo from traceSetup where serverNo= ? and attrib not like ?''',(svrNo, "%XXXUP"))
        data = list(cursor.fetchall())
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cursor.close()


def getupbitkey(uno):
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur17 = db.cursor()
    try:
        sql = "SELECT apiKey1, apiKey2 FROM pondUser WHERE userNo=%s and attrib not like %s"
        cur17.execute(sql, (uno, '%XXXUP'))
        data = cur17.fetchone()
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cur17.close()
        db.close()


def getupbitkey_tr(uno):
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur17 = db.cursor()
    try:
        sql = "SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo=%s and attrib not like %s"
        cur17.execute(sql, (uno, '%XXXUP'))
        data = cur17.fetchone()
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cur17.close()
        db.close()


def getupbitkey_trLoc(uno):
    connlc = sqlite3.connect("cpondLoc.db")
    cursor = connlc.cursor()
    try:
        cursor.execute('''SELECT apiKey1, apiKey2 FROM traceUser where userNo= ? and attrib not like ?''',(uno, "%XXXUP%"))
        data = list(cursor.fetchone())
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cursor.close()


def clearcache():
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur18 = db.cursor()
    sql = "RESET QUERY CACHE"
    cur18.execute(sql)
    cur18.close()
    db.close()


def setdetail(setno):
    global rows
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur20 = db.cursor()
    row = None
    try:
        sql = "SELECT * FROM tradingSets WHERE setNo = %s"
        cur20.execute(sql, setno)
        rows = cur20.fetchone()
    except Exception as e:
        print('접속오류', e)
    finally:
        cur20.close()
        db.close()
    return rows


def setdetail_tr(setno):
    global rows
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur20 = db.cursor()
    row = None
    try:
        sql = "SELECT * FROM traceSets WHERE setNo = %s"
        cur20.execute(sql, setno)
        rows = cur20.fetchone()
    except Exception as e:
        print('접속오류', e)
    finally:
        cur20.close()
        db.close()
    return rows


def setdetail_trLoc(setno):
    connlc = sqlite3.connect("cpondLoc.db")
    cursor = connlc.cursor()
    try:
        cursor.execute('''SELECT * FROM traceSets WHERE setNo = ?''', (setno,))
        data = list(cursor.fetchone())
        return data
    except Exception as e:
        print('접속오류', e)
    finally:
        cursor.close()


def errlog(err, userno):
    global rows
    db28 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur28 = db28.cursor()
    try:
        sql = "INSERT INTO error_Log (error_detail, userNo) VALUES (%s, %s)"
        cur28.execute(sql, (err, userno))
        db28.commit()
    except Exception as e:
        print('접속오류', e)
    finally:
        cur28.close()
        db28.close()


def servicelog(log, userno):
    global rows
    db30 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur30 = db30.cursor()
    try:
        sql = "INSERT INTO service_Log (service_detail, userNo) VALUES (%s, %s)"
        cur30.execute(sql, (log, userno))
        db30.commit()
    except Exception as e:
        print('접속오류 서비스로그', e)
    finally:
        cur30.close()
        db30.close()


def tradelog(uno, type, coinn, tstamp):
    global rows
    db32 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur32 = db32.cursor()
    try:
        if tstamp == "":
            tstamp = datetime.now()
        sql = "INSERT INTO tradeLog (userNo, tradeType, coinName, regDate) VALUES (%s, %s, %s, %s)"
        cur32.execute(sql, (uno, type, coinn, tstamp))
        db32.commit()
    except Exception as e:
        print('트레이드 로그실행 오류', e)
    finally:
        cur32.close()
        db32.close()


def getlog(uno, type, coinn):
    global rows
    db33 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur33 = db33.cursor()
    try:
        sql = "SELECT regDate FROM tradeLog where userNo = %s and attrib = %s and tradeType = %s and coinName = %s"
        cur33.execute(sql, (uno, '100001000010000', type, coinn))
        rows = cur33.fetchone()
    except Exception as e:
        print("트레이드 로그 조회 오류 ", e)
    finally:
        cur33.close()
        db33.close()
        return rows


def modifyLog(uuid, stat):
    global rows
    db34 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur34 = db34.cursor()
    try:
        sql = "UPDATE tradeLogDetail set attrib = %s where uuid = %s"
        if stat == "canceled":
            stat = "CANC0CANC0CANC0"
        elif stat == "confirmed":
            stat = "CONF0CONF0CONF0"
        else:
            stat = "UPD00UPD00UPD00"
        cur34.execute(sql, (stat, uuid))
        db34.commit()
    except Exception as e:
        print('거래 기록 업데이트 에러', e)
    finally:
        cur34.close()
        db34.close()


def insertLog(uno, ldata01, ldata02, ldata03, ldata04, ldata05, ldata06, ldata07, ldata08, ldata09, ldata10, ldata11,
              ldata12, ldata13, ldata14, ldata15, ldata16):
    global rows
    db35 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur35 = db35.cursor()
    try:
        sql = (
            "insert into tradeLogDetail (userNo,orderDate,uuid,side,ord_type,price,market,created_at,volume,remaining_volume,reserved_fee,paid_fee,locked,executed_volume,excuted_funds,trades_count,time_in_force)"
            " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        cur35.execute(sql, (
        uno, ldata01, ldata02, ldata03, ldata04, ldata05, ldata06, ldata07, ldata08, ldata09, ldata10, ldata11, ldata12,
        ldata13, ldata14, ldata15, ldata16))
        db35.commit()
    except Exception as e:
        print("거래 기록 인서트 에러", e)
    finally:
        cur35.close()
        db35.close()


def serviceStat(sno, sip, sver):
    global rows
    db36 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur36 = db36.cursor()
    try:
        sql = "INSERT INTO service_Stat (serverNo,serviceIp,serviceVer) VALUES (%s, %s, %s)"
        cur36.execute(sql, (sno, sip, sver))
        db36.commit()
    except Exception as e:
        print('접속상태 Log 기록 에러', e)
    finally:
        cur36.close()
        db36.close()


def getserverType(sno):
    global rows
    db37 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur37 = db37.cursor()
    try:
        sql = "select serviceType, serviceYN from serverSet where serverNo = %s and attrib not like %s"
        cur37.execute(sql, (sno, "XXXUP%"))
        rows = cur37.fetchone()
    except Exception as e:
        print('서버 서비스 조회', e)
    finally:
        cur37.close()
        db37.close()
        return rows


def lclog(coinn, uno, gap, lcamt, mywon, lossamt):
    global rows
    db38 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur38 = db38.cursor()
    try:
        sql = "INSERT INTO lcLog (lcCoinn,userNo,lcGap,lcAmt, remainKrw, lossAmt ) VALUES (%s, %s, %s, %s, %s, %s)"
        cur38.execute(sql, (coinn, uno, gap, lcamt, mywon, lossamt))
        db38.commit()
    except Exception as e:
        print('손절상태 Log 기록 에러', e)
    finally:
        cur38.close()
        db38.close()


def setonoff(sno, yesno):
    db = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur39 = db.cursor()
    connlc = sqlite3.connect("cpondLoc.db")
    cursor = connlc.cursor()
    try:
        sql = "UPDATE traceSetup SET activeYN = %s where setupNo=%s"
        cur39.execute(sql, (yesno, sno))
        db.commit()
        cursor.execute('''UPDATE traceSetup SET activeYN = ? where setupNo= ?''', (yesno, sno))
        connlc.commit()
    except Exception as e:
        print('상태 업데이트 오류', e)
    finally:
        cur39.close()
        db.close()
        cursor.close()

