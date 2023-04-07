import base64
import gzip
import hashlib
import hmac
import json
import os
import pathlib
from datetime import datetime
from io import BytesIO
from uuid import uuid4

import requests


def get_now_time_stamp() -> int:
    return int(datetime.timestamp(datetime.now()))


class GameAnalytics(object):
    """
    data structure must be a str(list(dict,dict,...)) single quote and double quote ate important
    sample_data = '[{"category": "design", "event_id": "coin:one", "amount": 1, "transaction_num": 10,
     "currency": "IRR","user_id":"9e457e68-3133-4ac1-8ad6-5c4c028eee5c", "sdk_version":"rest api v2",
     "os_version":"ios 11.4","manufacturer":"apple","device":"x86_64","platform":"ios",
     "session_id":"bd638487-1c88-4ce4-89f1-d6371f0996c2","session_num":20,"v":2}]'

    """
    _game_analytics = None

    content_encoding = 'gzip'
    event_design_storage = []
    event_business_storage = []
    event_category = {
        'business': "business",
        'design': "design",
    }

    def __init__(self,
                 base_url: str = 'https://api.gameanalytics.com',
                 event_url: str = None,
                 interval_seconds: int = 100,
                 game_analytics_game_key: str = os.getenv('GAME_ANALYTICS_GAME_KEY'),
                 game_analytics_secret_key: str = os.getenv('GAME_ANALYTICS_SECRET_KEY'),
                 design_storage_capacity: int = int(os.getenv('GAME_ANALYTICS_DESIGN_STORAGE', 100)),
                 business_storage_capacity: int = int(os.getenv('GAME_ANALYTICS_BUSINESS_STORAGE', 100)),
                 activation: bool = bool(int(os.getenv('GAME_ANALYTICS_ACTIVATION', 0))),
                 session=requests.session(),
                 event_file_path: str = f'{pathlib.Path.parent}/events_name.json'
                 ):
        self.event_id_json = self.open_events_name_json()
        self.session = session
        self.GAME_ANALYTICS_GAME_KEY = game_analytics_game_key
        self.GAME_ANALYTICS_SECRET_KEY = game_analytics_secret_key
        self.GAME_ANALYTICS_DESIGN_STORAGE = design_storage_capacity
        self.GAME_ANALYTICS_BUSINESS_STORAGE = business_storage_capacity
        self.GAME_ANALYTICS_ACTIVATION = activation
        self.EVENT_FILE_PATH = event_file_path
        self.base_url = base_url
        self.event_url = event_url if event_url else f'{self.base_url}/v2/{self.GAME_ANALYTICS_GAME_KEY}/events'
        self.interval_seconds = interval_seconds

    def open_events_name_json(self) -> dict:
        with open(self.EVENT_FILE_PATH, 'r') as f:
            data = json.load(f)
            return data

    @staticmethod
    def game_analytics():
        if not GameAnalytics._game_analytics:
            GameAnalytics._game_analytics = GameAnalytics()
        return GameAnalytics._game_analytics

    @staticmethod
    def get_gzip_string(string_for_gzip):
        zip_text_file = BytesIO()
        zipper = gzip.GzipFile(mode='wb', fileobj=zip_text_file)
        zipper.write(string_for_gzip.encode())
        zipper.close()
        enc_text = zip_text_file.getvalue()
        return enc_text

    @staticmethod
    def hmac_auth_hash(body_string, secret_key):
        sig_hash = hmac.new(secret_key.encode('utf8'), body_string, hashlib.sha256).digest()
        base64_message = base64.b64encode(sig_hash).decode()
        return base64_message

    def _authorization(self, content):
        hmac_key = self.hmac_auth_hash(
            body_string=self.get_gzip_string(content),
            secret_key=self.GAME_ANALYTICS_SECRET_KEY)

        return hmac_key

    def _params(self) -> dict:
        return {
            'game_key': self.GAME_ANALYTICS_GAME_KEY,
            'interval_seconds': self.interval_seconds,
        }

    def _header(self, gzip_data) -> dict:
        return {
            'authorization': self.hmac_auth_hash(body_string=gzip_data, secret_key=self.GAME_ANALYTICS_SECRET_KEY),
            'content-type': 'application/json',
            'content-encoding': self.content_encoding,
        }

    def send_event(self, data) -> requests.Response:
        data = self._prepare_data_to_send(x=data)
        data = str(data).replace('', "")
        gzip_data = self.get_gzip_string(string_for_gzip=data)
        r = self.session.post(
            url=self.event_url,
            headers=self._header(gzip_data=gzip_data),
            params=self._params(),
            data=gzip_data
        )
        return r

    @staticmethod
    def _prepare_data_to_send(x: list) -> str:
        return str(x).replace("\'", '\"')

    def _data(self, event_category: str, event_id: str, user_uuid: str, custom_field: dict = None) -> dict:
        if event_category not in self.event_category.keys():
            raise ValueError(f'event_category must be in {self.event_category.keys()}')
        if event_id not in self.event_id_json:
            raise ValueError(f'event_id not exists in events_name.json file')
        custom_field['time'] = get_now_time_stamp()
        necessary_fields = {  # necessary fields
            "category": event_category,
            "event_id": self.event_id_json[event_id],
            "user_id": str(user_uuid),
            "sdk_version": "rest api v2",
            "os_version": "ios 11.4",
            "manufacturer": "apple",
            "device": "x86_64",
            "platform": "ios",
            "session_id": uuid4().__str__(),
            "session_num": 20,
            "v": 2
        }
        return {**necessary_fields, **custom_field}

    def store_events(self, category, content: dict) -> None:
        if category == self.event_category['design']:
            storage = self.event_design_storage
            storage.append(content)
            if len(storage) >= self.GAME_ANALYTICS_DESIGN_STORAGE:
                self.send_event(data=storage)
                storage.clear()
        else:
            storage = self.event_business_storage
            storage.append(content)
            if len(storage) >= self.GAME_ANALYTICS_BUSINESS_STORAGE:
                self.send_event(data=storage)
                storage.clear()

    def event_business(self, event_id, user_uuid, amount, currency, transaction_num, **kwargs) -> None:
        if not self.GAME_ANALYTICS_ACTIVATION:
            return
        custom_field = {**kwargs, **{"amount": amount, "transaction_num": transaction_num, "currency": currency}}
        category = self.event_category['business']
        content = self._data(
            event_category=category,
            user_uuid=user_uuid,
            event_id=event_id,
            custom_field=custom_field,
        )
        self.store_events(category=category, content=content)
        return

    def event_design(self, event_id: str, user_uuid: str, custom_field: dict = None) -> None:
        if not self.GAME_ANALYTICS_ACTIVATION:
            return  # for developer information: around 4.00 ms proces time if inactivated
        category = self.event_category['design']
        content = self._data(
            event_category=self.event_category['design'],
            user_uuid=user_uuid,
            event_id=event_id,
            custom_field=custom_field
        )
        self.store_events(category=category, content=content)
        return
