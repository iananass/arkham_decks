import click
import sqlite3
from sys import stderr


@click.command()
@click.option('-c', '--card-id', multiple=True)
@click.option('-ic', '--investigator-code', multiple=False)
@click.option('-in', '--investigator-name', multiple=False)
@click.option('-af', '--alt-front', is_flag=True, multiple=False)
@click.option('-ab', '--alt-back', is_flag=True, multiple=False)
def main(card_id, investigator_code, investigator_name, alt_front, alt_back):
    # print(card_id)
    # print(investigator_code)
    # print(investigator_name)

    if investigator_code and investigator_name:
        print('ERROR: both investigator-code and investigator-name are defined', file=stderr)
        return

    condition_connector = 'where'
    condition = ''
    if card_id:
        for card in card_id:
            if condition:
                condition = f"(select id from decks_descr where id in {condition} and slot = '{card}')"
            else:
                condition = f"(select id from decks_descr where slot = '{card}')"
        condition = f" {condition_connector} id in {condition}"
        condition_connector = 'and'

    if investigator_code:
        condition += f' {condition_connector} investigator_code = {investigator_code}'
        condition_connector = 'and'
    elif investigator_name:
        condition += f' {condition_connector} investigator_name like \'%{investigator_name}%\''
        condition_connector = 'and'

    if alt_front:
        condition += f" {condition_connector} meta like '%alternate_front%' and meta not like '%alternate_front\":\"\"%'"
        condition_connector = 'and'
    if alt_back:
        condition += f" {condition_connector} meta like '%alternate_back%' and meta not like '%alternate_back\":\"\"%'"
        condition_connector = 'and'

    request = f"select id, investigator_name from decks {condition}"

    with sqlite3.connect(f'arkham.db') as local_db:
        cur = local_db.cursor()
        rows_count = 0
        for deck_id, investigator_name in cur.execute(request):
            rows_count += 1
            if rows_count > 51:
                pass
            elif rows_count > 50:
                print(' . . . ')
            else:
                print(f'https://ru.arkhamdb.com/decklist/view/{deck_id} {investigator_name}')
        print(f'{rows_count} decks found')


if __name__ == '__main__':
    main()

