#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import psycopg2
import logging
import os
from requests import Request, Session, codes
from bs4 import BeautifulSoup

pg_timeout=5

parser = argparse.ArgumentParser(description='Customs info updater.')
parser.add_argument('--host', type=str, help='PG host')
parser.add_argument('--db', type=str, help='database name')
parser.add_argument('--user', type=str, help='db user')
parser.add_argument('--log', type=str, default="INFO", help='log level')
args = parser.parse_args()

# password='PASS'-.pgpass
DSN = 'dbname=%s host=%s user=%s' % (args.db, args.host, args.user)

log_name = os.path.splitext(os.path.basename(__file__))[0] + '.log'

numeric_level = getattr(logging, args.log, None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % numeric_level)
logging.basicConfig(filename=log_name, format='%(asctime)s %(levelname)s: %(message)s', level=numeric_level) # INFO)

try:
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED, autocommit=True)
    curs = conn.cursor()
except BaseException, exc:
    logging.warning(" Exception on connect=%s. Sleep for %s", str(exc), str(pg_timeout))
else:
    logging.debug("before select")
    select_cmd = 'SELECT DISTINCT "КодТНВЭД" FROM "Содержание" WHERE "КодТНВЭД" IS NOT NULL;'
    curs.execute(select_cmd)
    tnved_list = curs.fetchall()
    # DEBUG tnved_list = [('8541300009',)]

    #for i, (items) in enumerate(tnved_list):
    #    print "N={0}, код={1}".format(i, items[0])

    sess = Session()

    for tnved_code in tnved_list:
        url = 'http://www.tks.ru/db/tnved/tree/c{0}/print'.format(tnved_code[0]) 
        req = Request('GET', url)
        prepped = sess.prepare_request(req)
        resp = None
        try:
            resp = sess.send(prepped)
            logging.debug("got page by tnved_code")
            logging.debug("resp.status_code=%s", resp.status_code)
            # logging.debug("resp.text=%s", resp.text)

            if codes.ok != resp.status_code:
                logging.warning("http code NEQ 200, sess.headers=%s, sess.params=%s", str(sess.headers),  str(sess.params))
                continue

            # soup = BeautifulSoup(resp.text.decode('utf8'), 'html.parser')
            soup = BeautifulSoup(resp.text, 'html.parser')

            # logging.debug(soup.get_text())
            duty_next = False
            import_section = False
            dual_use_next = False
            prev_str = ''
            dual_use_str = ''
            import_duty = None
            for t in [text.encode('utf8') for text in soup.stripped_strings]:
                if 'ИМПОРТ' == t:
                    import_section = True
                    tnved_name = prev_str.strip('-').strip()
                    logging.debug('tnved_name=[%s]', tnved_name)
                if 'ЭКСПОРТ' == t:
                    import_section = False
                if duty_next:
                    duty_next = False
                    import_duty = 0.0
                    correct_duty = True
                    if 'Нет' != t:
                        correct_duty = False
                        duty_str = t.replace(' %', '')
                        try:
                            import_duty = float(duty_str)
                        except ValueError:
                            logging.error('BAD duty_str=[%s], source=[%s]', duty_str, t)
                        else:
                            correct_duty = True
                    logging.debug('import_duty=%s', import_duty)
                if dual_use_next:
                    dual_use_next = False
                    dual_use_str = t
                if 'Импортная пошлина' == t:
                    duty_next = True
                if 'Двойное применение' == t:
                    dual_use_next = True
                prev_str = t

            if import_duty is not None:
                # logging.debug("before insert_cmd")
                insert_cmd = """INSERT INTO tnved(tnved_code, import_duty, tnved_name, dual_use, src_url, url_response)
                VALUES(%s, %s, %s, %s, %s, %s) 
                ON CONFLICT (tnved_code,import_duty,tnved_name) DO UPDATE SET url_response = excluded.url_response, dual_use = ' 
                """ + dual_use_str + "';"
                # logging.debug("insert_cmd={0}".format(insert_cmd))
                curs.execute(insert_cmd, (tnved_code, import_duty, tnved_name, dual_use_str, url, resp.text))
            else:
                logging.warning("import_duty is None for tnved_code={0}".format(tnved_code))


        except Exception as e:
            logging.critical("exception=%s", str(e))

    curs.close()
    conn.close()
