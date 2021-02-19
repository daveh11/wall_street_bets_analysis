from psaw import PushshiftAPI
import config
import datetime
import psycopg2
import psycopg2.extras
import re

todays_date = datetime.date.today()

connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
cursor.execute("""
    SELECT * FROM stock
""")
rows = cursor.fetchall()

stocks = {}
for row in rows:
    stocks[row['symbol']] = row['id']


api = PushshiftAPI()

start_time = int(datetime.datetime(todays_date.year, todays_date.month, todays_date.day -10).timestamp())

submissions = api.search_submissions(after=start_time,
                            subreddit='wallstreetbets',
                            filter=['url','author', 'title', 'subreddit'],
                            )



for submission in submissions:
#    caps = []
#    try:
#        caps = re.findall('([A-Z]+(?=\s[A-Z]+)(?:\s[A-Z]+)+)', submission.title)
#        caps = caps[0].split()
#        print('--------------------------------{}-------------------'.format(caps))
#    except Exception:
#        pass
#
    words = submission.title.split()
    cashtags = list(set(filter(lambda word: word.lower().startswith('$'), words)))
#    cashtags.extend(caps)

    if len(cashtags) > 0:
        print(cashtags)
        print(submission.title)

        for cashtag in cashtags:
            cashtag = cashtag.replace('$', '')
            cashtag = cashtag.title()
            if cashtag in stocks:
                submitted_time = datetime.datetime.fromtimestamp(submission.created_utc).isoformat()
                print(cashtag)

                try:
                    cursor.execute("""
                        INSERT INTO mention (dt, stock_id, message, source, url)
                        VALUES (%s, %s, %s, 'wallstreetbets', %s)
                    """, (submitted_time, stocks[cashtag], submission.title, submission.url))

                    connection.commit()
                except Exception as e:
                    print(e)
                    connection.rollback()
