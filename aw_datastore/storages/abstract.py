import logging
from typing import List, Dict, Optional
from datetime import datetime
from abc import ABCMeta, abstractmethod, abstractproperty

from aw_core.models import Event


class AbstractStorage(metaclass=ABCMeta):
    """
    Interface for storage methods.
    """

    sid = "Storage id not set, fix me"

    @abstractmethod
    def __init__(self, testing: bool) -> None:
        self.testing = True
        raise NotImplementedError

    @abstractmethod
    def buckets(self) -> Dict[str, dict]:
        raise NotImplementedError

    @abstractmethod
    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_bucket(self, bucket_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, bucket_id: str) -> dict:
        raise NotImplementedError

    @abstractmethod
    def get_event(
        self,
        bucket_id: str,
        event_id: int,
    ) -> Optional[Event]:
        raise NotImplementedError

    @abstractmethod
    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> List[Event]:
        raise NotImplementedError

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def insert_one(self, bucket_id: str, event: Event) -> Event:
        raise NotImplementedError

    @abstractmethod
    def insert_many(self, bucket_id: str, events: List[Event]) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, bucket_id: str, event_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def replace(self, bucket_id: str, event_id: int, event: Event) -> bool:
        raise NotImplementedError

    @abstractmethod
    def replace_last(self, bucket_id: str, event: Event) -> None:
        raise NotImplementedError

    @abstractmethod
    def save_user(self, user_data):
        raise NotImplementedError

    @abstractmethod
    def get_user(self, filter):
        raise NotImplementedError  
    
    @abstractmethod
    def get_all_users(self):
        raise NotImplementedError

    @abstractmethod
    def get_use_tracker(self):
        raise NotImplementedError

    @abstractmethod
    def save_report(self):
        raise NotImplementedError

    @abstractmethod
    def get_report(self):
        raise NotImplementedError
