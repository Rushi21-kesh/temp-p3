# import the Shrimpy library for crypto exchange websockets
import shrimpy
from kafka import KafkaProducer
from time import sleep
import json

# a sample error handler, it simply prints the incoming error
def error_handler(err):
    print(err)

# define the handler to manage the output stream
cryptoData = {}
def handler(msg):
    # multiple trades can be returned in each message, so take the last one
    cryptoData['pair'] = msg['pair']
    cryptoData['value'] = str(msg['content'][len(msg['content']) - 1]['price'])
    data = json.dumps(cryptoData)
    producer.send('crypto', data)
    #print(cryptoData)
    producer.flush()
    sleep(5)

# input your Shrimpy public and private key
public_key = 'd3413c69127ce382111239e3bf398845cfc9f93536d883d8bf1de604ec12562d'
private_key = 'd354c846fb8421bfbd4f40ea8349fd7721045179b46213f9c00312b0189d7d81d62c607eca3dfced6e20c0f031a87c5178057ed0468779bb41eb3c49bdf319ea'

# create the Shrimpy websocket client
api_client = shrimpy.ShrimpyApiClient(public_key, private_key)
raw_token = api_client.get_token()
client = shrimpy.ShrimpyWsClient(error_handler, raw_token['token'])
producer = KafkaProducer(value_serializer = lambda x: str(x).encode("utf-8"), bootstrap_servers = ['localhost:9092'])

# construct the subscription object
subscribe_btc = {
    "type": "subscribe",
    "exchange": "binance",
    "pair": "btc-usdt",
    "channel": "trade"
}
subscribe_eth = {
    "type": "subscribe",
    "exchange": "binance",
    "pair": "eth-usdt",
    "channel": "trade"
}
subscribe_ltc = {
    "type": "subscribe",
    "exchange": "binance",
    "pair": "ltc-usdt",
    "channel": "trade"
}
subscribe_bch = {
    "type": "subscribe",
    "exchange": "binance",
    "pair": "bch-usdt",
    "channel": "trade"
}

# connect to the Shrimpy websocket and subscribe
client.connect()
client.subscribe(subscribe_btc, handler)
client.subscribe(subscribe_ltc, handler)
client.subscribe(subscribe_eth, handler)
client.subscribe(subscribe_bch, handler)

# disconnect from the websocket once data collection is complete
client.disconnect()