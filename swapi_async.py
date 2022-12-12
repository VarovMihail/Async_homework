from typing import Iterator, AsyncIterator, Coroutine

from aiohttp import ClientSession
import json
import asyncio
import time
from more_itertools import chunked
from db import engine, Session, People, Base

CHUNK_SIZE = 10

async def get_person(id):
    """Получить одного персонажа и исправить его словарь"""
    async with ClientSession() as session:
        response = await session.get(f'https://swapi.dev/api/people/{id}')

    person_dict = await response.json()

    if person_dict.get('name'):
        person_dict['vehicles'] = ','.join(person_dict['vehicles'])
        person_dict['starships'] = ','.join(person_dict['starships'])
        person_dict['species'] = ','.join(person_dict['species'])
        person_dict['films'] = ','.join(person_dict['films'])
        del person_dict['created']
        del person_dict['edited']
        del person_dict['url']
        return person_dict
    else:
        print('пустой персонаж под номером', id)
        return {}


async def chunk_people(start: int, end: int) -> AsyncIterator[dict]:
    """Разбить количество героев на куски и сделать генератор"""
    for chunk in chunked(range(start, end), CHUNK_SIZE):
        coroutines: list[Coroutine] = [get_person(i) for i in chunk]
        people = await asyncio.gather(*coroutines)
        for person_dict in people:
            yield person_dict

async def add_people_to_db(person_dict_buffer):
    """Вставить кусок в базу данных"""
    async with Session() as session:

        people_instance = [People(**person_dict) for person_dict in person_dict_buffer]
        print(len(people_instance), 'героев грузим в базу')
        session.add_all(people_instance)
        await session.commit()

async def main():
    """Основная функция"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
        person_dict_buffer = []

    async for person_dict in chunk_people(1, 84):
        if person_dict:
            person_dict_buffer.append(person_dict)

        if len(person_dict_buffer) >= 10:
            asyncio.create_task(add_people_to_db(person_dict_buffer))
            person_dict_buffer.clear()

    if person_dict_buffer:
        await add_people_to_db(person_dict_buffer)

    tasks = set(asyncio.all_tasks())
    tasks = tasks - {asyncio.current_task()}
    for task in tasks:
        await task

    await engine.dispose()

asyncio.run(main())
