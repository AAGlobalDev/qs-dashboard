import quandl
import pandas as pd
import datetime
import psycopg2
import json
import os
###---------------------------------------------------------------------------------------------------------------------------------------------------
#Remove ARGS
def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

###---------------------------------------------------------------------------------------------------------------------------------------------------
#Insert API Key
quandl.ApiConfig.api_key = 'wakoMoaSST3pXnJjAWc6'

###---------------------------------------------------------------------------------------------------------------------------------------------------

#Configure Date Window
startdate = '2020-04-01'
enddate = datetime.date.today()
daterange = pd.date_range(startdate, enddate).tolist()
daterange=[str(i) for i in daterange]



#5 Delta Put Implied Volatility
IV5D10 = 'dlt5iv10d'
IV5D20 = 'dlt5iv20d'
IV5D30 = 'dlt5iv30d'
IV5D60 = 'dlt5iv60d'
IV5D90 = 'dlt5iv90d'
IV5D180 = 'dlt5iv6m'
IV5D360 = 'dlt5iv1y'

#25 Delta Put Implied Volatility
IV25D10 = 'dlt25iv10d'
IV25D20 = 'dlt25iv20d'
IV25D30 = 'dlt25iv30d'
IV25D60 = 'dlt25iv60d'
IV25D90 = 'dlt25iv90d'
IV25D180 = 'dlt25iv6m'
IV25D360 = 'dlt25iv1y'

#ATM Implied Volatility
IV50D10 = 'iv10d'
IV50D20 = 'iv20d'
IV50D30 = 'iv30d'
IV50D60 = 'iv60d'
IV50D90 = 'iv90d'
IV50D180 = 'iv6m'
IV50D360 = 'iv1yr'

#25 Delta Call Implied Volatility
IV75D10 = 'dlt75iv10d'
IV75D20 = 'dlt75iv20d'
IV75D30 = 'dlt75iv30d'
IV75D60 = 'dlt75iv60d'
IV75D90 = 'dlt75iv90d'
IV75D180 = 'dlt75iv6m'
IV75D360 = 'dlt75iv1y'

#5 Delta Call Implied Volatility
IV95D10 = 'dlt95iv10d'
IV95D20 = 'dlt95iv20d'
IV95D30 = 'dlt95iv30d'
IV95D60 = 'dlt95iv60d'
IV95D90 = 'dlt95iv90d'
IV95D180 = 'dlt95iv6m'
IV95D360 = 'dlt95iv1y'

###---------------------------------------------------------------------------------------------------------------------------------------------------
#Import Realized Volatility Data

#Close to Close
HVC10 = 'clshv10d'
HVC20 = 'clshv20d'
HVC60 = 'clshv60d'
HVC120 = 'clshv120d'
HVC252 = 'clshv252d'

#Intra-day Volatility
HVID10 = 'orhv10d'
HVID20 = 'orhv20d'
HVID60 = 'orhv60d'
HVID120 = 'orhv120d'
HVID252 = 'orhv252d'

###---------------------------------------------------------------------------------------------------------------------------------------------------
#Import OI and Volume Data

#Volume Data
CallV = 'cvolu'
PutV = 'pvolu'
AVGTOV20 = 'avgoptvolu20d'

#Open Interest Data
CallOI = 'coi'
PutOI = 'poi'

###---------------------------------------------------------------------------------------------------------------------------------------------------
#Stock List

stocks = ['IGV','SMH','SPY','QQQ','IWM','RSX','FEZ','EWA','EWC','EWI','EWG', 'EWH','EWJ','EWL','EWM','EWP','EWU','EWW','EWY','EWZ','EZA','FXI']

def main():
###
    skews10d = [IV5D10, IV25D10, IV50D10, IV75D10, IV95D10]
    TenDayIV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks,
                                qopts={'columns':['ticker','tradedate',skews10d]}, paginate=True)
    blankIndex=[''] * len(TenDayIV)
    TenDayIV.index=blankIndex
    TenDayIV = TenDayIV.pivot_table(index=['tradedate'], columns=['ticker'], values= skews10d).fillna(0)
###
    skews20d = [IV5D20, IV25D20, IV50D20, IV75D20, IV95D20]
    TwentyDayIV = quandl.get_table('ORATS/VOL', tradedate=daterange, ticker=stocks,
                                   qopts={'columns': ['ticker', 'tradedate', skews20d]}, paginate=True)
    blankIndex = [''] * len(TwentyDayIV)
    TwentyDayIV.index = blankIndex
    TwentyDayIV = TwentyDayIV.pivot_table(index=['tradedate'],
                                          columns=['ticker'], values=skews20d).fillna(0)
###
    skews30d = [IV5D30,IV25D30,IV50D30,IV75D30,IV95D30]
    ThirtyDayIV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks,
                                   qopts={'columns':['ticker','tradedate',skews30d]}, paginate=True)
    blankIndex=[''] * len(ThirtyDayIV)
    ThirtyDayIV.index=blankIndex
    ThirtyDayIV = ThirtyDayIV.pivot_table(index=['tradedate'], columns=['ticker'], values= skews30d).fillna(0)
####
    skews60d = [IV5D60,IV25D60,IV50D60,IV75D60,IV95D60]
    SixtyDayIV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks,
                                  qopts={'columns':['ticker','tradedate',skews60d]}, paginate=True)
    blankIndex=[''] * len(SixtyDayIV)
    SixtyDayIV.index=blankIndex
    SixtyDayIV = SixtyDayIV.pivot_table(index=['tradedate'], columns=['ticker'], values= skews60d).fillna(0)
####
    skews90d = [IV5D90,IV25D90,IV50D90,IV75D90,IV95D90]
    NinetyDayIV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks,
                                   qopts={'columns':['ticker','tradedate',skews90d]}, paginate=True)
    blankIndex=[''] * len(NinetyDayIV)
    NinetyDayIV.index=blankIndex
    NinetyDayIV = NinetyDayIV.pivot_table(index=['tradedate'], columns=['ticker'], values= skews90d).fillna(0)
####
    skews180d = [IV5D180,IV25D180,IV50D180,IV75D180,IV95D180]
    SixMonthIV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks,
                                  qopts={'columns':['ticker','tradedate',skews180d]}, paginate=True)
    blankIndex=[''] * len(SixMonthIV)
    SixMonthIV.index=blankIndex
    SixMonthIV = SixMonthIV.pivot_table(index=['tradedate'], columns=['ticker'], values= skews180d).fillna(0)
####
    skews360d = [IV5D360,IV25D360,IV50D360,IV75D360,IV95D360]
    OneYearIV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks,
                                 qopts={'columns':['ticker','tradedate',skews360d]}, paginate=True)
    blankIndex=[''] * len(OneYearIV)
    OneYearIV.index=blankIndex
    OneYearIV = OneYearIV.pivot_table(index=['tradedate'], columns=['ticker'], values= skews360d).fillna(0)
####
    ClosetoCloseRV = [HVC10,HVC20,HVC60,HVC120,HVC252]
    CCRV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks,
                            qopts={'columns':['ticker','tradedate',ClosetoCloseRV]}, paginate=True)
    blankIndex=[''] * len(CCRV)
    CCRV.index=blankIndex
    CCRV = CCRV.pivot_table(index=['tradedate'], columns=['ticker'], values= ClosetoCloseRV).fillna(0)
###
    IntradayRV = [HVID10,HVID20,HVID60,HVID120,HVID252]
    IDRV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks, qopts={'columns':['ticker','tradedate',IntradayRV]}, paginate=True)
    blankIndex=[''] * len(IDRV)
    IDRV.index=blankIndex
    IDRV = IDRV.pivot_table(index=['tradedate'], columns=['ticker'], values= IntradayRV).fillna(0)
####

    TenDayATM = TenDayIV[IV50D10]
    TenDayIntraDayRV = IDRV[HVID10]
    TenDayIntraDayIVRVSpread = TenDayATM.subtract(TenDayIntraDayRV, fill_value=0)

    TwentyDayATM = TwentyDayIV[IV50D20]
    TwentyDayIVTenDayIntraDayRVSpread = TwentyDayATM.subtract(TenDayIntraDayRV, fill_value=0)

    ThirtyDayATM = ThirtyDayIV[IV50D30]
    ThirtyDayIVTenDayIntraDayRVSpread = ThirtyDayATM.subtract(TenDayIntraDayRV, fill_value=0)

    SixtyDayATM = SixtyDayIV[IV50D60]
    SixtyDayIVTenDayIntraDayRVSpread = SixtyDayATM.subtract(TenDayIntraDayRV, fill_value=0)

    NinetyDayATM = NinetyDayIV[IV50D90]
    NinetyDayIVTenDayIntraDayRVSpread = NinetyDayATM.subtract(TenDayIntraDayRV, fill_value=0)

    SixMonthATM = SixMonthIV[IV50D180]
    SixMonthIVTenDayIntraDayRVSpread = SixMonthATM.subtract(TenDayIntraDayRV, fill_value=0)

    TenDayClosetoCloseRV = CCRV[HVC10]
    TenDayIVRVSpread = TenDayATM.subtract(TenDayClosetoCloseRV, fill_value=0)

    TwentyDayIVTenDayClosetoCloseRVSpread = TwentyDayATM.subtract(TenDayClosetoCloseRV, fill_value=0)

    ThirtyDayIVTenDayClosetoCloseRVSpread = ThirtyDayATM.subtract(TenDayClosetoCloseRV, fill_value=0)

    SixtyDayIVTenDayClosetoCloseRVSpread = SixtyDayATM.subtract(TenDayClosetoCloseRV, fill_value=0)

    NinetyDayIVTenDayClosetoCloseRVSpread = NinetyDayATM.subtract(TenDayClosetoCloseRV, fill_value=0)

    SixMonthIVTenDayClosetoCloseRVSpread = SixMonthATM.subtract(TenDayClosetoCloseRV, fill_value=0)


    TenDayIntraDayIVRVSpread.index = TenDayIntraDayIVRVSpread.index.strftime('%m/%d/%Y')


    date_list = TenDayIntraDayIVRVSpread.index.tolist()
    stocks_list = TenDayIntraDayIVRVSpread.columns.values.tolist()


    intra10d = TenDayIntraDayIVRVSpread.values.tolist()
    intra20d = TwentyDayIVTenDayIntraDayRVSpread.values.tolist()
    intra30d = ThirtyDayIVTenDayIntraDayRVSpread.values.tolist()
    intra60d = SixtyDayIVTenDayIntraDayRVSpread.values.tolist()
    intra90d = NinetyDayIVTenDayIntraDayRVSpread.values.tolist()
    intra6m = SixMonthIVTenDayIntraDayRVSpread.values.tolist()

    close10d = TenDayIVRVSpread.values.tolist()
    close20d = TwentyDayIVTenDayClosetoCloseRVSpread.values.tolist()
    close30d = ThirtyDayIVTenDayClosetoCloseRVSpread.values.tolist()
    close60d = SixtyDayIVTenDayClosetoCloseRVSpread.values.tolist()
    close90d = NinetyDayIVTenDayClosetoCloseRVSpread.values.tolist()
    close6m = SixMonthIVTenDayClosetoCloseRVSpread.values.tolist()

    #values_list = TenDayIntraDayIVRVSpread.values.tolist()


    # with open('config.json') as json_file:
    #    config = json.load(json_file)
    # con = psycopg2.connect(
    #    host=config['REDSHIFT_HOST'],
    #    port=config['REDSHIFT_PORT'],
    #    database=config['REDSHIFT_DBNAME'],
    #    user=config['REDSHIFT_USERNAME'],
    #    password=config['REDSHIFT_PASSWORD']
    # )

    con = psycopg2.connect(
       host=os.environ['REDSHIFT_HOST'],
       port=os.environ['REDSHIFT_PORT'],
       database=os.environ['REDSHIFT_DBNAME'],
       user=os.environ['REDSHIFT_USERNAME'],
       password=os.environ['REDSHIFT_PASSWORD']
    )

    # con = psycopg2.connect(
    #     host='database-1.clyjrfzdyg83.us-east-2.rds.amazonaws.com',
    #     port='5432',
    #     database='mydb',
    #     user='postgres',
    #     password='superstar123')

    cur = con.cursor()
    cur.execute('''DROP TABLE IF EXISTS demotable;''')
    con.commit()

    cur = con.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS demotable (ticker CHAR(5), tradedate DATE, intra10d FLOAT, 
        intra20d FLOAT, intra30d FLOAT, intra60d FLOAT, intra90d FLOAT, intra6m FLOAT, close10d FLOAT, 
        close20d FLOAT, close30d FLOAT, close60d FLOAT, close90d FLOAT, close6m FLOAT);''')
    con.commit()

    ipos = 0
    jpos = 0
    for tradedate in date_list:
        for ticker in stocks_list:
            print("{}={}+++++++++++++++".format(jpos, ipos))
            val_intra10d = intra10d[jpos][ipos]
            print("{}__{}__{}___".format(ticker, tradedate, val_intra10d))
            val_intra20d = intra20d[jpos][ipos]
            val_intra30d = intra30d[jpos][ipos]
            val_intra60d = intra60d[jpos][ipos]
            val_intra90d = intra90d[jpos][ipos]
            val_intra6m = intra6m[jpos][ipos]
            val_close10d = close10d[jpos][ipos]
            val_close20d = close20d[jpos][ipos]
            val_close30d = close30d[jpos][ipos]
            val_close60d = close60d[jpos][ipos]
            val_close90d = close90d[jpos][ipos]
            val_close6m = close6m[jpos][ipos]

            cur.execute(
                "INSERT INTO demotable (ticker, tradedate, intra10d, intra20d, intra30d, intra60d, intra90d, intra6m, "
                "close10d, close20d, close30d, close60d, close90d, close6m) "
                "VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}'"
                ", '{13}')".format(ticker, tradedate, val_intra10d, val_intra20d, val_intra30d, val_intra60d,
                                   val_intra90d, val_intra6m, val_close10d, val_close20d, val_close30d,
                                   val_close60d, val_close90d, val_close6m));
            con.commit()

            ipos = ipos + 1
        jpos = jpos + 1
        ipos = 0

    con.commit()
    con.close()


def handler(event, context):
    try:
        now = datetime.datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Started at =", current_time)

        main()

        now = datetime.datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Ended at =", current_time)
    except Exception as e:
        print(e)


if __name__ == "__main__":

    try:
        now = datetime.datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Time =", current_time)

        main()

        now = datetime.datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Time =", current_time)
    except Exception as e:
        print(e)
