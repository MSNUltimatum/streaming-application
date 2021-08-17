import argparse
import json
import os
import random
import datetime
from typing import NoReturn, Generator, Dict, TextIO, List

post_ids: List[int] = list(range(3000, 3020))
ips_public_range: List[int] = list(range(11, 172))


def generate_logs() -> NoReturn:
    current_date_time: str = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    file_name: str = f"{args.file}_{current_date_time}.json"
    file_path = os.path.join('./resources', 'requests', file_name)
    with open(file_path, 'w') as fw:
        fw.write("[")
        generate(fw)
        fw.write("]")


def generate(fw: TextIO):
    start_time: datetime.datetime = datetime.datetime.now()
    requests_generator: Generator = generate_events(start_time)
    for event in requests_generator:
        json.dump(event, fw)
        fw.write(",\n")


def generate_events(start_time: datetime.datetime) -> Generator:
    start, end = start_time, start_time + datetime.timedelta(seconds=args.duration)
    while start < end:
        for user_id in generate_users_ids_per_sec():
            yield generate_user_event(start, user_id)

        for bot_id in range(0, args.bots):
            bot_actions_count = random.randint(0, 5)
            for bot_event in generate_bot_events(start, bot_id, bot_actions_count):
                yield bot_event
        start += datetime.timedelta(seconds=1)


def generate_users_ids_per_sec() -> List[int]:
    users_ids: range = range(0, args.users)
    return random.sample(users_ids, args.freq)


def generate_user_event(dt: datetime.datetime, user_id: int) -> Dict:
    user_ip: str = generate_ip(user_id, 16)
    return {'eventTime': int(dt.timestamp()),
            'eventType': generate_user_event_type(),
            'ip': user_ip,
            'url': generate_url()}


def generate_bot_events(dt: datetime.datetime, bot_id: int, bot_actions_count: int) -> Generator:
    for action in range(0, bot_actions_count):
        yield generate_bot_event(dt, bot_id)


def generate_bot_event(dt: datetime.datetime, bot_id: int) -> Dict:
    bot_ip: str = generate_ip(bot_id, 17)
    return {'eventTime': int(dt.timestamp()),
            'eventType': generate_bot_event_type(),
            'ip': bot_ip,
            'url': generate_url()}


def generate_user_event_type() -> str:
    return random.choice(['click', 'view', 'view', 'view'])


def generate_bot_event_type() -> str:
    return random.choice(['click', 'click', 'click', 'view'])


def generate_ip(user_id: int, second_octet: int) -> str:
    first_octet: int = ips_public_range[user_id % len(ips_public_range)]
    third_octet: int = user_id % 255
    fourth_octet: int = user_id // 255
    return f'{first_octet}.{second_octet}.{third_octet}.{fourth_octet}'


def generate_url() -> str:
    post_id: int = random.choice(post_ids)
    return f'https://zen.yandex.ru/post/{post_id}'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--duration', type=int, default=100, help="generation duration")
    parser.add_argument('-f', '--file', type=str, default='req_data', help="file name")
    parser.add_argument('-u', '--users', type=int, default=1000, help="users count")
    parser.add_argument('-b', '--bots', type=int, default=5, help="bots count")
    parser.add_argument('--freq', type=int, default=100, help="user requests per second count")
    args = parser.parse_args()
    generate_logs()
