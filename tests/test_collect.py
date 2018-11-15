# coin_market_cap_api_key = a0f43282-0bdd-457b-b65b-02939a74c270

import ccxt
import datetime


binance = ccxt.binance()
print(binance.id)
start=1527836
limit=480
pair='eth/btc'.upper()
d = binance.fetch_ohlcv(pair,
                        timeframe='1h',
                        since=start,
                        limit=limit)
print(d)
