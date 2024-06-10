import sqlite3
import json
import http.client
from time import time
import os
import threading


class Slot:
    def __init__(self, card_id, count):
        self.card_id = card_id
        self.count = count

    def __repr__(self):
        return f'{self.card_id}x{self.count}'


class Deck:
    def __init__(self, js_str):
        js = json.loads(js_str)
        self.id = js['id']
        self.investigator_name = js['investigator_name']
        self.investigator_code = int(js['investigator_code'])
        self.user_id = js['user_id']
        self.slots = []
        self.meta = js["meta"]
        self.taboo_id = js["taboo_id"]
        for card_id, count in js['slots'].items():
            self.slots.append(Slot(card_id, count))

    def insert_data(self):
        return self.id, self.investigator_code, self.investigator_name, self.user_id, self.meta, self.taboo_id


def load_deck(connection, deck_id):
    url = f'https://ru.arkhamdb.com/api/public/decklist/{deck_id}.json'
    connection.request('GET', url)
    response = connection.getresponse()
    if response.status != 200:
        print(f'Error: http code {response.status}, {response.reason} at deck {deck_id}')
    js_str = response.read().decode('utf-8')
    if not js_str:
        return None
    d = Deck(js_str)

    # print(deck)
    # print(d.id, d.investigator_name, d.slots)
    return d


def create_decks():
    return 'create table if not exists decks (id int, investigator_code int, investigator_name text, user_id text, meta text, taboo_id text, PRIMARY KEY (id))'


def create_decks_descr():
    return 'create table if not exists decks_descr (id int, slot text, count int, PRIMARY KEY (id, slot))'


def load_one_piece(start, end):
    with sqlite3.connect(f'deck_list_{start}_{end}.db') as local_db:
        cur = local_db.cursor()
        cur.execute(create_decks())
        cur.execute(create_decks_descr())
        local_db.commit()

        time_start = time()
        count_per_min = 0

        for deck_id in range(start, end):
            conn = http.client.HTTPSConnection('ru.arkhamdb.com', 443)
            deck = load_deck(conn, deck_id)
            if deck:
                slots = [(deck.id, s.card_id, s.count) for s in deck.slots]
                cur.execute(f'insert or ignore into decks values (?, ?, ?, ?, ?, ?)', deck.insert_data())
                cur.executemany('insert or ignore into decks_descr values (?, ?, ?)', slots)
                local_db.commit()
            count_per_min += 1
            now = time()
            if now - time_start >= 60:
                print(f'{count_per_min} decks per minute')
                count_per_min = 0
                time_start = now
        cur.close()


def main():
    pieces_num = 15
    last_deck = 50000

    pieces = [(int(i*last_deck/pieces_num + 1), int((i+1)*last_deck/pieces_num)) for i in range(pieces_num)]

    threads = [threading.Thread(target=load_one_piece, args=p) for p in pieces]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    merged_db_name = 'arkham.db'
    try:
        os.remove(merged_db_name)
    except FileNotFoundError:
        pass
    os.system(f'sqlite3 {merged_db_name} \'{create_decks()}\'')
    os.system(f'sqlite3 {merged_db_name} \'{create_decks_descr()}\'')

    for start, end in pieces:
        filename = f'deck_list_{start}_{end}.db'
        print(filename)
        os.system(f'sqlite3 {merged_db_name} "attach \'{filename}\' as toMerge; '
                  f'insert into decks select * from toMerge.decks; '
                  f'insert into decks_descr select * from toMerge.decks_descr"')


main()
