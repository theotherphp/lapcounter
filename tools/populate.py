import random
import time
import websocket

def create_ws():
    return websocket.create_connection('ws://mini.local:8080/laps')

ws = create_ws()
for i in range(20000):
    time.sleep(2)
    try:
        _ = ws.send(str(random.choice(range(200))).zfill(4))
    except:
        try:
            ws = create_ws()
        except:
            print 'retrying in 3s'
            time.sleep(3)
