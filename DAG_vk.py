import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import vk_api
import re
from nltk.corpus import stopwords
import psycopg2

TOKEN = "fa267307fa267307fa2673074ffa50e889ffa26fa2673079a11a84f2cb2d50acb0df095"
HOST = "postgresdm.cqwsq96j6gje.us-east-1.rds.amazonaws.com"
DATA = "postgres"
PORT = "5432"
GROUP_ID = "-35488145"

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2021, 3, 16),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
}

def start():
    session = vk_api.VkApi(token=TOKEN)
    api = session.get_api()
    posts = get_posts(group_id=GROUP_ID, api=api)
    map = {}
    for item in posts:
        words = item.get('text').split(' ')
        for word in words:
            word = word.lower()
            if re.match("[a-zA-Zа-яА-ЯёЁ#_]+", word) is not None:
                word = re.match("[a-zA-Zа-яА-ЯёЁ#_]+", word).group()
                if not word in stopwords.words('russian'):
                    if map.get(word) is None:
                        map[word] = 1
                    else:
                        map[word] = map[word] + 1
    map = {k: v for k, v in sorted(map.items(),
                                         key=lambda item: item[1],
                                         reverse=True)}

    conn = psycopg2.connect(
        database=DATA,
        user=DATA,
        password=DATA,
        host=HOST,
        port=PORT
    )
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS words_statistic")
    cursor.execute("""create table words_statistic (
                        word  varchar not null,
                        count integer not null
                    );""")

    for (key, value) in map.items():
         cursor.execute("INSERT INTO words_statistic (word, count) VALUES (\'{}\', {})".format(str(key), value))

    # cursor.executemany("INSERT INTO words_statistic (word, count) VALUES (%(key)s, %(value)s)", map)
    conn.commit()
    conn.close()

def get_posts(group_id, api):
    posts_0_100 = api.wall.get(owner_id=group_id, count=100).get('items')
    posts_101_200 = api.wall.get(owner_id=group_id, count=100, offset=100).get('items')
    return posts_0_100 + posts_101_200


with DAG(dag_id='dag_vk', default_args=args, schedule_interval=None) as dag:
    parse_vk_wall = PythonOperator(
        task_id='words_statistic',
        python_callable=start,
        dag=dag
    )
