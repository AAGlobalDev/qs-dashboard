#Import Requirements

import quandl
import pandas as pd
import datetime
import psycopg2
import os
import time
import json

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

###---------------------------------------------------------------------------------------------------------------------------------------------------
#Build Data Frames by Expiry and Skew Point
###---------------------------------------------------------------------------------------------------------------------------------------------------

#10 day Implied by Skew Point
skews10d = [IV5D10,IV25D10,IV50D10,IV75D10,IV95D10]


def handler():
    TenDayIV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks, qopts={'columns':['ticker','tradedate',skews10d]}, paginate=True)

    #TenDayIV.to_csv (r'export_dataframe.csv', index = False, header=True)

    blankIndex=[''] * len(TenDayIV)
    TenDayIV.index=blankIndex

    TenDayIV = TenDayIV.pivot_table(index=['tradedate'], columns=['ticker'], values= skews10d).fillna(0)


    ###---------------------------------------------------------------------------------------------------------------------------------------------------
    #Build Data Frames for Realized Volatility
    ###---------------------------------------------------------------------------------------------------------------------------------------------------
    #Intra-Day Realized

    IntradayRV = [HVID10,HVID20,HVID60,HVID120,HVID252]

    IDRV = quandl.get_table('ORATS/VOL', tradedate= daterange, ticker= stocks, qopts={'columns':['ticker','tradedate',IntradayRV]}, paginate=True)

    #IDRV.to_csv (r'IDRV.csv', index = False, header=True)
    blankIndex=[''] * len(IDRV)
    IDRV.index=blankIndex

    IDRV = IDRV.pivot_table(index=['tradedate'], columns=['ticker'], values= IntradayRV).fillna(0)

    ###---------------------------------------------------------------------------------------------------------------------------------------------------
    #Build IV-RV Analysis DataFrames
    ###---------------------------------------------------------------------------------------------------------------------------------------------------

    #10 Day Close to Close vs 10 Day ATM IV
    #Calculate 10D ATM IV
    TenDayATM = TenDayIV[IV50D10]

    #10 Day Intra-Day RV vs 10 Day IV
    #Calculate 10D Intra-day RV
    TenDayIntraDayRV = IDRV[HVID10]
    #Find the Difference between IV and RV
    TenDayIntraDayIVRVSpread = TenDayATM.subtract(TenDayIntraDayRV, fill_value=0)

    # print(TenDayIntraDayIVRVSpread.squeeze())
    ###--------------------------------------------------------------------------------------------------------------------------------------------------
    #Heatmap Outputssns.set(font_scale=2) # font size 2
    ###--------------------------------------------------------------------------------------------------------------------------------------------------

    #10 Day ATM IV vs 10 Day RV (Intra-Day)
    TenDayIntraDayIVRVSpread.index = TenDayIntraDayIVRVSpread.index.strftime('%m/%d/%Y')

    date_list = TenDayIntraDayIVRVSpread.index.tolist()
    # print(date_list)
    stocks_list = TenDayIntraDayIVRVSpread.columns.values.tolist()
    # print(stocks_list)
    values_list = TenDayIntraDayIVRVSpread.values.tolist()
    # print(values_list)


    with open('config.json') as json_file:
        config = json.load(json_file)
    con = psycopg2.connect(
        host=config['REDSHIFT_HOST'],
        port=config['REDSHIFT_PORT'],
        database=config['REDSHIFT_DBNAME'],
        user=config['REDSHIFT_USERNAME'],
        password=config['REDSHIFT_PASSWORD']
    )

    # con = psycopg2.connect(host="database-1.clyjrfzdyg83.us-east-2.rds.amazonaws.com",
    #                            port=5432, database="postgres", user="postgres", password="superstar123")

    ############ Drop Table If Exists...
    cur = con.cursor()
    cur.execute('''DROP TABLE IF EXISTS demotable;''')
    con.commit()

    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS demotable (ticker CHAR(5), tradedate DATE, value FLOAT);''')
    con.commit()

    ipos = 0
    jpos = 0
    for tradedate in date_list:
        for ticker in stocks_list:
            print("{}={}+++++++++++++++".format(jpos, ipos))
            value = values_list[jpos][ipos]
            print("{}__{}__{}___".format(ticker, tradedate, value))

            cur.execute(
                "INSERT INTO demotable (ticker, tradedate, value) "
                "VALUES ('{0}', '{1}', '{2}')".format(ticker, tradedate, value));
            con.commit()

            ipos = ipos + 1
        jpos = jpos + 1
        ipos = 0

    con.commit()
    con.close()


handler()