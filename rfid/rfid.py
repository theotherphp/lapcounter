"""
Read and write RFID tags for the Relay app
"""

import argparse
from functools import partial
import logging
from signal import signal, SIGTERM, SIGINT
import sys
import time
import websocket

import mercury

logging.basicConfig(
    name=__name__,
    filename='rfid.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

ws = None
dedup_cache = {}
DEDUP_THRESHOLD = 5.0

class RelayWebsocket(object):

    def __init__(self, host):
        self.tag_buffer = []
        self.good = False
        self.host = host

    def send(self, tag):
        self.tag_buffer.append(tag)
        if not self.good:
            try:
                logging.info('connecting')
                self.ws = websocket.create_connection('ws://' + self.host + ':8080/laps')
                self.ws.on_close = self.on_close
                self.good = True
            except Exception as e:
                logging.error('_connect: %s' % str(e))
        if self.good:
            tags = ','.join(self.tag_buffer)
            num_tags = len(self.tag_buffer)
            if num_tags > 1:
                logging.debug('sending %d tags' % num_tags)
            try:
                self.ws.send(tags)
                self.tag_buffer = []
            except Exception as e:
                self.good = False
                logging.error('send: %s' % str(e))

    def close(self):
        logging.debug('close')
        if self.good:
            self.ws.close()
        self.good = False

    def on_close(self):
        logging.debug('on_close')
        self.good = False


def post(epc_obj):
    epc = repr(epc_obj).strip('\'')  # ugh
    hex_numbers = [epc[i:i+2] for i in range(0, len(epc), 2)]
    chars = [chr(int(ch, 16)) for ch in hex_numbers]
    tag = ''.join(chars)
    now = time.time()
    if now - dedup_cache.get(tag, 0.0) > DEDUP_THRESHOLD:
        dedup_cache[tag] = now
        if ws:
            ws.send(tag)
    else:
        logging.debug('duplicate read %s' % tag)


def sig_handler(sig, frame):
    logging.info('caught signal %d' % sig)
    sys.exit(0)


if __name__ == '__main__':
    logging.info('starting')
    reader = None

    signal(SIGTERM, partial(sig_handler))
    signal(SIGINT, partial(sig_handler))

    parser = argparse.ArgumentParser(description='Relay RFID reader/writer')
    parser.add_argument('--write-range', default='', help='batch write tags')
    parser.add_argument('--host', default='relay.local', help='host name of relay server')
    ns = parser.parse_args()

    try:
        reader = mercury.Reader('tmr:///dev/ttyUSB0')
        reader.set_region('NA2')
        pwr = 500 if ns.write_range else 2600  # hundredths of dBm
        reader.set_read_plan([1], 'GEN2', read_power=pwr)
        if ns.write_range:
            rng = ns.write_range.split('-')
            for t in range(int(rng[0]), int(rng[1]) + 1):
                time.sleep(5)
                reader.write(str(t).zfill(4))
                logging.info('wrote %d' % t)
        else:
            reader.start_reading(post, on_time=250, off_time=250)
            ws = RelayWebsocket(ns.host)
    except Exception as e:
        logging.error(str(e))

    try:
        if not ns.write_range:
            while True:
                time.sleep(60)
    finally:
        logging.info('exiting')
        if reader:
            reader.stop_reading()
        if ws:
            ws.close()
