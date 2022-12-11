# WS client example

import asyncio
import websockets
import json
import httpx
import csv


subscriber = '{"action":"subscribe_symbol", "key":"QUOTE_COINBASE_SPOT_ETH_BTC"}'


def get_ws_token() -> str:
    server = 'wss://atz3h3bzji.execute-api.eu-west-2.amazonaws.com/development?token='
    authUrl = 'https://maz5ef1wy1.execute-api.eu-west-2.amazonaws.com/development/auth'
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    auth_data = json.dumps({'username': 'ai.websocket', 'password': 'Nintendo321*'})
    res = httpx.post(authUrl, headers=headers, data=auth_data)
    tokenResponse = res.json()
    token = tokenResponse['AuthenticationResult']['AccessToken']
    return "{}{}".format(server, token)


async def consumer():
    url = get_ws_token()

    async with websockets.connect(url) as ws:
        await ws.send(subscriber)
        with open('ETH_BTC.csv', 'a', newline='') as file:
            async for msg in ws:
                if "Successfully subscribed!" in msg:
                    continue
                data = json.loads(msg)
                writer = csv.writer(file)
                writer.writerow(['id', 'symbol', 'sequence', 'ask_price', 'ask_size', 'bid_price', 'bid_size', 'time_coinapi', 'time_exchange', 'ts'])
                writer.writerow([data['id'], data['symbolId'], data['sequence'], data['askPrice'], data['askSize'], data['bidPrice'], data['bidSize'], data['timeCoinApi'], data['timeExchange'],])
                print(">>> ", data)


if __name__ == "__main__":
    
    asyncio.get_event_loop().run_until_complete(consumer())



# {"sequence":1419776,"symbolId":"QUOTE_COINBASE_SPOT_ETH_BTC","askPrice":0.07402,"partitionKey":"QUOTE_COINBASE_SPOT_ETH_BTC","timeExchange":"2022-12-11T21:31:57.643460Z","bidSize":3.99,"id":"42571199-98d4-4800-b4c8-1a265508bc19","timeCoinApi":"2022-12-11T21:31:57.711019700Z","askSize":6.77717595,"bidPrice":0.07399}