import requests
import time

def get_person(id):
    print(requests.get(f'https://swapi.dev/api/people/{id}').json())

def main():
    for i in range(1, 18):
        get_person(i)

start = time.monotonic()
main()
print(time.monotonic() - start)