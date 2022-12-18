from typing import AsyncIterator, Coroutine
import requests
from aiohttp import ClientSession
import asyncio
from more_itertools import chunked
from db import engine, Session, People, Base
import time


CHUNK_SIZE = 10

async def my_list(link_dict: dict):
    """Асинхронные запросы по ссылкам films, vehicles, starships, species"""
    list_of_dicts = []
    for link in link_dict:
        async with ClientSession() as session:
            response = await session.get(link)
        item_dict = await response.json()
        print(type(item_dict))
        list_of_dicts.append(item_dict)
    return list_of_dicts


async def get_person(person_id: int) -> dict:
    """Получить одного персонажа и исправить его словарь"""
    async with ClientSession() as session:
        response = await session.get(f'https://swapi.dev/api/people/{person_id}')

    person_dict = await response.json()

    if person_dict.get('name'):
        person_dict['films'] = ','.join([i['title'] for i in await my_list(person_dict['films'])])
        person_dict['vehicles'] = ','.join([i['name'] for i in await my_list(person_dict['vehicles'])])
        person_dict['starships'] = ','.join([i['name'] for i in await my_list(person_dict['starships'])])
        person_dict['species'] = ','.join([i['name'] for i in await my_list(person_dict['species'])])
        del person_dict['created']
        del person_dict['edited']
        del person_dict['url']
        return person_dict
    else:
        print('пустой персонаж под номером', person_id)
        return {}


async def chunk_people(start: int, end: int) -> AsyncIterator[dict]:
    """Разбить количество героев на куски и сделать генератор"""
    for chunk in chunked(range(start, end), CHUNK_SIZE):
        coroutines: list[Coroutine] = [get_person(i) for i in chunk]
        people: tuple = await asyncio.gather(*coroutines)
        for person_dict in people:
            yield person_dict


async def add_people_to_db(person_dict_buffer: list):
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
            # person_dict_buffer.clear()
            # В работе с памятью перемудрили. отчищается тот же самый список,
            # который идет на вставку, которая идет паралельно с основным кодом.
            # Просто создавайте новый пустой список
            person_dict_buffer = []

    if person_dict_buffer:
        await add_people_to_db(person_dict_buffer)

    tasks = set(asyncio.all_tasks())
    tasks = tasks - {asyncio.current_task()}
    for task in tasks:
        await task

    await engine.dispose()


if __name__ == '__main__':
    start = time.monotonic()
    asyncio.run(main())
    print(time.monotonic() - start)