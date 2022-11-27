from fastapi import FastAPI
from binance.spot import Spot as Client
import pandas as pd
import ta
import csv
import threading
import json
from candlestick import candlestick
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
client = Client()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_crypto_symbols(random=True, amount=5):
    """Get crypto symbols from csv file"""
    if random:
        # get 5 random symbols from cryptos.csv
        symbols = pd.read_csv('cryptos.csv')
        symbols = symbols.sample(amount)
        symbols = symbols['Symbol'].tolist()
        return symbols
    if random == False:
        # get all symbols from cryptos.csv
        symbols = pd.read_csv('cryptos.csv')
        symbols = symbols['Symbol'].tolist()
        return symbols


def csv2json_indicators(csvfile, jsonfile):
    csvfile = open(csvfile, 'r')
    jsonfile = open(jsonfile, 'w')

    fieldnames = ("symbol", "close", "bb_high_price_%", "rsi", "roc", "donchian_channel_wband", "sma200_price_%",
                  "sma50_price_%", "sma200_sma50_%", "ema9_price_%", "ema9_sma50_%", "cumulative_return")

    reader = csv.DictReader(csvfile, fieldnames)

    # create an empty list to store the dictionaries
    out = []

    # remove the header
    next(reader, None)

    # loop through the csv file and add each row to the list
    for row in reader:
        out.append(row)

    json.dump(out, jsonfile, indent=4)

    csvfile.close()
    jsonfile.close()


def csv2json_candlesticks(csvfile, jsonfile):
    csvfile = open(csvfile, 'r')
    jsonfile = open(jsonfile, 'w')

    fieldnames = ("symbol", "inverted_hammer", "hammer", "hanging_man", "bearish_harami",
                  "bullish_harami", "dark_cloud_cover", "doji", "doji_star", "dragonfly_doji", "gravestone_doji", "bearish_engulfing", "bullish_engulfing", "morning_star", "morning_star_doji", "piercing_pattern", "rain_drop", "rain_drop_doji", "star", "shooting_star")

    reader = csv.DictReader(csvfile, fieldnames)

    # create an empty list to store the dictionaries
    out = []

    # remove the header
    next(reader, None)

    # loop through the csv file and add each row to the list
    for row in reader:
        out.append(row)

    json.dump(out, jsonfile, indent=4)

    csvfile.close()
    jsonfile.close()


def calculate_indicators(symbols, filename, timeframe):

    with open(filename, 'w') as f:
        write = csv.writer(f)
        write.writerow(['Symbol', 'Close', 'bb_high_price_%', 'rsi', 'roc', 'donchian_channel_wband',
                       'sma200_price_%', 'sma50_price_%', 'sma200_sma50_%', 'ema9_price_%', 'ema9_sma50_%', 'cumulative_return'])

        for symbol in symbols:

            df = pd.DataFrame(client.klines(symbol, timeframe, limit=300))
            df = df.iloc[:, :9]
            df.columns = ['Time', 'Open', 'High', 'Low', 'Close',
                          'Volume', 'Close_time', 'Quote_av', 'Trades']
            # drop close_time and Quete_av
            df = df.drop(['Close_time', 'Quote_av', 'Trades'], axis=1)
            # convert to datetime
            df['Time'] = pd.to_datetime(df['Time'], unit='ms')
            # set index
            df = df.set_index('Time')
            # convert to float
            df = df.astype(float)
            # #########################################################################################
            # add bb top line
            indicator_bb = ta.volatility.BollingerBands(
                close=df["Close"], window=20, window_dev=2)
            df['bb_high_price_%'] = (
                (df['Close'] - indicator_bb.bollinger_hband()) / indicator_bb.bollinger_hband() * 100).round(2)
            # add rsi
            df['rsi'] = ta.momentum.RSIIndicator(
                close=df["Close"], window=14).rsi().round(4)
            # add roc
            df['roc'] = ta.momentum.ROCIndicator(
                close=df["Close"], window=12).roc().round(4)
            # add donchian channel
            indicator_dc = ta.volatility.DonchianChannel(
                high=df["High"], low=df["Low"], close=df["Close"], window=20)
            df['donchian_channel_wband'] = indicator_dc.donchian_channel_wband().round(2)
            # add kc
            # indicator_kc = ta.volatility.KeltnerChannel(high=df["High"], low=df["Low"], close=df["Close"], window=20, window_atr=10)
            # df['kc_high_price_%'] = ((df['Close'] - indicator_kc.keltner_channel_hband()) / indicator_kc.keltner_channel_hband() * 100).round(2)
            # df['kc_h'] = indicator_kc.keltner_channel_hband()
            # # add macd
            # indicator_macd = ta.trend.MACD(close=df["Close"], window_slow=26, window_fast=12, window_sign=9)
            # df['macd_%'] = ((indicator_macd.macd() - indicator_macd.macd_signal()) / indicator_macd.macd() * 100).round(2)
            # add moving averages without ta lib
            df['sma200'] = df['Close'].rolling(window=200).mean()
            df['sma50'] = df['Close'].rolling(window=50).mean()
            df['ema9'] = df['Close'].ewm(span=9, adjust=False).mean()

            df['sma200_price_%'] = (
                (df['Close'] - df['sma200']) / df['sma200'] * 100).round(2)
            df['sma50_price_%'] = (
                (df['Close'] - df['sma50']) / df['sma50'] * 100).round(2)
            df['sma200_sma50_%'] = (
                (df['sma200'] - df['sma50']) / df['sma200'] * 100).round(2)
            df['ema9_price_%'] = (
                (df['Close'] - df['ema9']) / df['ema9'] * 100).round(2)
            df['ema9_sma50_%'] = (
                (df['ema9'] - df['sma50']) / df['ema9'] * 100).round(2)
            # add cumulative return
            df['cumulative_return'] = ta.others.CumulativeReturnIndicator(
                close=df["Close"]).cumulative_return().round(2)
            ############################################################################################
            # remove all rows exept the last one
            df = df.iloc[-1:]
            print(symbol, df)
            write.writerow([symbol, df['Close'].values[0], df['bb_high_price_%'].values[0], df['rsi'].values[0], df['roc'].values[0], df['donchian_channel_wband'].values[0], df['sma200_price_%'].values[0],
                           df['sma50_price_%'].values[0], df['sma200_sma50_%'].values[0], df['ema9_price_%'].values[0], df['ema9_sma50_%'].values[0], df['cumulative_return'].values[0]])


def calculate_candlesticks(symbols, filename, timeframe):

    with open(filename, 'w') as f:
        write = csv.writer(f)
        write.writerow(['Symbol', 'inverted_hammer', 'hammer', 'hanging_man', 'bearish_harami',
                       'bullish_harami', 'dark_cloud_cover', 'doji', 'doji_star', 'dragonfly_doji', 'gravestone_doji', 'bearish_engulfing', 'bullish_engulfing', 'morning_star', 'morning_star_doji', 'piercing_pattern', 'rain_drop', 'rain_drop_doji', 'star', 'shooting_star'])

        for symbol in symbols:

            df = pd.DataFrame(client.klines(symbol, timeframe, limit=5))
            df = df.iloc[:, :9]
            df.columns = ['Time', 'Open', 'High', 'Low', 'Close',
                          'Volume', 'Close_time', 'Quote_av', 'Trades']
            # drop close_time and Quete_av
            df = df.drop(['Close_time', 'Quote_av', 'Trades'], axis=1)
            # convert to datetime
            df['Time'] = pd.to_datetime(df['Time'], unit='ms')
            # set index
            df = df.set_index('Time')
            # convert to float
            df = df.astype(float)
            # #########################################################################################
            # add candlestick patterns
            df = candlestick.inverted_hammer(df, target='inverted_hammer', is_reversed=False,  ohlc=[
                                             'Open', 'High', 'Low', 'Close'])
            df = candlestick.hammer(df, target='hammer', is_reversed=False,  ohlc=[
                                    'Open', 'High', 'Low', 'Close'])
            df = candlestick.hanging_man(df, target='hanging_man', is_reversed=False,  ohlc=[
                                         'Open', 'High', 'Low', 'Close'])
            df = candlestick.bearish_harami(df, target='bearish_harami', is_reversed=False,  ohlc=[
                                            'Open', 'High', 'Low', 'Close'])
            df = candlestick.bullish_harami(df, target='bullish_harami', is_reversed=False,  ohlc=[
                                            'Open', 'High', 'Low', 'Close'])
            df = candlestick.dark_cloud_cover(df, target='dark_cloud_cover', is_reversed=False,  ohlc=[
                                              'Open', 'High', 'Low', 'Close'])
            df = candlestick.doji(df, target='doji', is_reversed=False,  ohlc=[
                                  'Open', 'High', 'Low', 'Close'])
            df = candlestick.doji_star(df, target='doji_star', is_reversed=False,  ohlc=[
                                       'Open', 'High', 'Low', 'Close'])
            df = candlestick.dragonfly_doji(df, target='dragonfly_doji', is_reversed=False,  ohlc=[
                                            'Open', 'High', 'Low', 'Close'])
            df = candlestick.gravestone_doji(df, target='gravestone_doji', is_reversed=False,  ohlc=[
                                             'Open', 'High', 'Low', 'Close'])
            df = candlestick.bearish_engulfing(df, target='bearish_engulfing', is_reversed=False,  ohlc=[
                                               'Open', 'High', 'Low', 'Close'])
            df = candlestick.bullish_engulfing(df, target='bullish_engulfing', is_reversed=False,  ohlc=[
                                               'Open', 'High', 'Low', 'Close'])
            df = candlestick.morning_star(df, target='morning_star', is_reversed=False,  ohlc=[
                                          'Open', 'High', 'Low', 'Close'])
            df = candlestick.morning_star_doji(df, target='morning_star_doji', is_reversed=False,  ohlc=[
                                               'Open', 'High', 'Low', 'Close'])
            df = candlestick.piercing_pattern(df, target='piercing_pattern', is_reversed=False,  ohlc=[
                                              'Open', 'High', 'Low', 'Close'])
            df = candlestick.rain_drop(df, target='rain_drop', is_reversed=False,  ohlc=[
                                       'Open', 'High', 'Low', 'Close'])
            df = candlestick.rain_drop_doji(df, target='rain_drop_doji', is_reversed=False,  ohlc=[
                                            'Open', 'High', 'Low', 'Close'])
            df = candlestick.star(df, target='star', is_reversed=False,  ohlc=[
                                  'Open', 'High', 'Low', 'Close'])
            df = candlestick.shooting_star(df, target='shooting_star', is_reversed=False,  ohlc=[
                                           'Open', 'High', 'Low', 'Close'])
            ############################################################################################
            # remove all rows exept the last one
            df = df.iloc[-1:]
            print(symbol, df)
            write.writerow([symbol, df['inverted_hammer'].values[0], df['hammer'].values[0], df['hanging_man'].values[0], df['bearish_harami'].values[0],
                           df['bullish_harami'].values[0], df['dark_cloud_cover'].values[0], df['doji'].values[0], df['doji_star'].values[0], df['dragonfly_doji'].values[0], df['gravestone_doji'].values[0], df['bearish_engulfing'].values[0], df['bullish_engulfing'].values[0], df['morning_star'].values[0], df['morning_star_doji'].values[0], df['piercing_pattern'].values[0], df['rain_drop'].values[0], df['rain_drop_doji'].values[0], df['star'].values[0], df['shooting_star'].values[0]])


@app.get("/indicators")
async def root():

    symbols = get_crypto_symbols(random=False)

    t1 = threading.Thread(target=calculate_indicators,
                          args=(symbols, 'indicators1d.csv', '1d'))
    t2 = threading.Thread(target=calculate_indicators,
                          args=(symbols, 'indicators4h.csv', '4h'))
    t3 = threading.Thread(target=calculate_indicators, args=(
        symbols, 'indicators30min.csv', '30m'))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()

    t1 = threading.Thread(target=csv2json_indicators,
                          args=('indicators1d.csv', 'indicators1d.json'))
    t2 = threading.Thread(target=csv2json_indicators,
                          args=('indicators4h.csv', 'indicators4h.json'))
    t3 = threading.Thread(target=csv2json_indicators, args=(
        'indicators30min.csv', 'indicators30min.json'))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()

    return {"message": "zopa"}


@app.get("/indicators/daily")
async def daily():

    with open('indicators1d.json') as f:
        data = json.load(f)

    return {"data": data}


@app.get("/indicators/4h")
async def four_hours():

    with open('indicators4h.json') as f:
        data = json.load(f)

    return {"data": data}


@app.get("/indicators/30min")
async def thirty_minutes():

    with open('indicators30min.json') as f:
        data = json.load(f)

    return {"data": data}


@app.get("/candlesticks")
async def candlesticks():

    symbols = get_crypto_symbols(random=False)

    t1 = threading.Thread(target=calculate_candlesticks,
                          args=(symbols, 'candlesticks1d.csv', '1d'))
    t2 = threading.Thread(target=calculate_candlesticks,
                          args=(symbols, 'candlesticks4h.csv', '4h'))
    t3 = threading.Thread(target=calculate_candlesticks, args=(
        symbols, 'candlesticks30min.csv', '30m'))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()

    t1 = threading.Thread(target=csv2json_candlesticks,
                          args=('candlesticks1d.csv', 'candlesticks1d.json'))
    t2 = threading.Thread(target=csv2json_candlesticks,
                          args=('candlesticks4h.csv', 'candlesticks4h.json'))
    t3 = threading.Thread(target=csv2json_candlesticks, args=(
        'candlesticks30min.csv', 'candlesticks30min.json'))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()

    return {"message": "zopa"}

@app.get("/candlesticks/daily")
async def daily():

    with open('candlesticks1d.json') as f:
        data = json.load(f)

    return {"data": data}


@app.get("/candlesticks/4h")
async def four_hours():

    with open('candlesticks4h.json') as f:
        data = json.load(f)

    return {"data": data}


@app.get("/candlesticks/30min")
async def thirty_minutes():

    with open('candlesticks30min.json') as f:
        data = json.load(f)

    return {"data": data}