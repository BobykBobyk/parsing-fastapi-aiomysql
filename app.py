from fastapi import FastAPI, Query
from requests import get
from bs4 import BeautifulSoup
import aiomysql

app = FastAPI()

db_pool = None


async def get_db_pool():
    return await aiomysql.create_pool(
        host='localhost',
        port=3306,
        user='root',
        password='password',
        db='project_database',
        minsize=5,
        maxsize=10
    )


async def create_database():
    pool = await get_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute('CREATE DATABASE IF NOT EXISTS project_database;')
            await connection.commit()


async def create_table():
    pool = await get_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS tegs (
            id INT AUTO_INCREMENT PRIMARY KEY, 
            teg VARCHAR(255),
            content TEXT)
            );
        """)
    await connection.commit()


@app.on_event('startup')
async def startup_event():
    await create_database()
    await create_table()


@app.get('/data')
async def get_data(url: str = Query(...),
                   teg: str = Query(...)
                   ):
    response = get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    results = []

    for i in soup.find_all(teg):
        results.append(str(i.text.strip()))

    pool = await get_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            for content in results:
                page_name = url.split('/')[-1]
                await cursor.execute(
                    'INSERT INTO tegs(teg, page_name, content) VALUES (%s, %s, %s)',
                    (teg, page_name, content)
                )
        await connection.commit()

    return {'result': results}
