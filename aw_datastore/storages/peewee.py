from email.policy import default
import time
from typing import Optional, List, Dict, Any
from datetime import date, datetime, timezone, timedelta
import json
import os
import logging
import iso8601

import peewee
from peewee import (
    Model,
    CharField,
    IntegerField,
    DecimalField,
    DateTimeField,
    ForeignKeyField,
    AutoField,
    FloatField,
    BooleanField
)
from playhouse.postgres_ext import *

from aw_core.models import Event
from aw_core.dirs import get_data_dir

from .abstract import AbstractStorage
from playhouse.migrate import *

logger = logging.getLogger(__name__)

# Prevent debug output from propagating
peewee_logger = logging.getLogger("peewee")
peewee_logger.setLevel(logging.INFO)

# Init'd later in the PeeweeStorage constructor.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#run-time-database-configuration
# Another option would be to use peewee's Proxy.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#dynamic-db
_db = PostgresqlExtDatabase(
    'komutracker',  # Required by Peewee.
    user='komutracker',  # Will be passed directly to psycopg2.
    password='1q2w#E$R',  # Ditto.
    host='localhost')  # Ditto.


LATEST_VERSION = 2


def chunks(ls, n):
    """Yield successive n-sized chunks from ls.
    From: https://stackoverflow.com/a/312464/965332"""
    for i in range(0, len(ls), n):
        yield ls[i : i + n]


def dt_plus_duration(dt, duration):
    # See peewee docs on datemath: https://docs.peewee-orm.com/en/latest/peewee/hacks.html#date-math
    return peewee.fn.strftime(
        "%Y-%m-%d %H:%M:%f+00:00",
        (peewee.fn.julianday(dt) - 2440587.5) * 86400.0 + duration,
        "unixepoch",
    )

def dt_plus_duration_postgres(dt, duration):
    # See peewee docs on datemath: https://docs.peewee-orm.com/en/latest/peewee/hacks.html#date-math
    return SQL(f"{dt} + make_interval(secs => {duration})")


class BaseModel(Model):
    class Meta:
        database = _db


class BucketModel(BaseModel):
    class Meta:
        table_name = 'buckets'
        
    key = AutoField(primary_key=True)
    id = CharField(unique=True)
    created = DateTimeTZField(default=datetime.now)
    name = CharField(null=True)
    type = CharField()
    client = CharField()
    hostname = CharField()

    def json(self):
        return {
            "id": self.id,
            "created": "",
            "name": self.name,
            "type": self.type,
            "client": self.client,
            "hostname": self.hostname,
        }

class UserModel(BaseModel):
    class Meta:
        table_name = 'users'

    id = AutoField()
    device_id = CharField()
    name = CharField()
    email = CharField(unique=True)
    mezon_user_id = CharField(unique=True)
    access_token = CharField(max_length=4096)
    refresh_token = CharField(max_length=4096)
    last_used_at = DateTimeTZField(null=True)

    def json(self):
        return {
            "id": self.id,
            "device_id": self.device_id,
            "name": self.name,
            "email": self.email,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "mezon_user_id": self.mezon_user_id,
            "last_used_at": self.last_used_at
        }


class EventModel(BaseModel):
    class Meta:
        table_name = 'events'

    id = AutoField()
    bucket = ForeignKeyField(BucketModel, backref="events", index=True)
    timestamp = DateTimeTZField(index=True, default=datetime.now)
    duration = IntervalField()
    datastr = TextField()

    @classmethod
    def from_event(cls, bucket_key, event: Event):
        return cls(
            bucket=bucket_key,
            id=event.id,
            timestamp=event.timestamp,
            duration=f"S {event.duration.total_seconds()}",
            datastr=json.dumps(event.data),
        )

    def json(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "duration": self.duration,
            "data": json.loads(self.datastr),
        }

class ReportModel(BaseModel):
    class Meta:
        table_name = 'reports'
    id = AutoField()
    email = CharField()
    spent_time = FloatField()
    call_time = FloatField()
    date = DateTimeTZField(index=True, default=datetime.now)
    wfh = BooleanField()

    def json(self):
        return {
            "id": self.id,
            "email": self.email,
            "spent_time": self.spent_time,
            "call_time": self.call_time,
            "date": self.date,
            "wfh": self.wfh
        }


class PeeweeStorage(AbstractStorage):
    sid = "peewee"

    def __init__(self, testing: bool = True, filepath: str = None) -> None:
        # data_dir = get_data_dir("aw-server")

        # if not filepath:
            # filename = (
            #     "peewee-sqlite"
            #     + ("-testing" if testing else "")
            #     + f".v{LATEST_VERSION}"
            #     + ".db"
            # )
            # filepath = os.path.join(data_dir, filename)
        self.db = _db
        # self.db.init(filepath)
        logger.info(f"Using database file: {filepath}")

        self.db.connect()

        self.bucket_keys: Dict[str, int] = {}
        BucketModel.create_table(safe=True)
        EventModel.create_table(safe=True)
        UserModel.create_table(safe=True)
        ReportModel.create_table(safe=True)
        
        migrator = PostgresqlMigrator(self.db)
        if any('last_used_at' == users_column.name for users_column in self.db.get_columns('users')):
            pass
        else:
            migrate(migrator.add_column('users', 'last_used_at', DateTimeTZField(null=True)))
        # migrate(migrator.drop_column('users', 'last_used_at'))

        self.update_bucket_keys()
        self.cached_buckets = None
        self.last_cached_ms = 0

    def update_bucket_keys(self) -> None:
        buckets = BucketModel.select()
        self.bucket_keys = {bucket.id: bucket.key for bucket in buckets}

    def buckets(self) -> Dict[str, Dict[str, Any]]:
        # if time.time() - self.last_cached_ms < 60000:
        #     print("cached")
        #     return self.cached_buckets
        # print("first time buckets")
        buckets = {bucket.id: bucket.json() for bucket in BucketModel.select()}
        # self.cached_buckets = buckets
        # self.last_cached_ms = time.time()
        return buckets

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
    ):
        BucketModel.create(
            id=bucket_id,
            type=type_id,
            client=client,
            hostname=hostname,
            created=created,
            name=name,
        )
        self.update_bucket_keys()

    def delete_bucket(self, bucket_id: str) -> None:
        if bucket_id in self.bucket_keys:
            EventModel.delete().where(
                EventModel.bucket == self.bucket_keys[bucket_id]
            ).execute()
            BucketModel.delete().where(
                BucketModel.key == self.bucket_keys[bucket_id]
            ).execute()
            self.update_bucket_keys()
        else:
            raise Exception("Bucket did not exist, could not delete")

    def get_metadata(self, bucket_id: str):
        if bucket_id in self.bucket_keys:
            return BucketModel.get(
                BucketModel.key == self.bucket_keys[bucket_id]
            ).json()
        else:
            raise Exception("Bucket did not exist, could not get metadata")

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        e = EventModel.from_event(self.bucket_keys[bucket_id], event)
        e.save()
        event.id = e.id
        return event

    def insert_many(self, bucket_id, events: List[Event]) -> None:
        # NOTE: Events need to be handled differently depending on
        #       if they're upserts or inserts (have id's or not).

        # These events are updates which need to be applied one by one
        events_updates = [e for e in events if e.id is not None]
        for e in events_updates:
            self.insert_one(bucket_id, e)

        # These events can be inserted with insert_many
        events_dictlist = [
            {
                "bucket": self.bucket_keys[bucket_id],
                "timestamp": event.timestamp,
                "duration": f"S {event.duration.total_seconds()}",
                "datastr": json.dumps(event.data),
            }
            for event in events
            if event.id is None
        ]

        # Chunking into lists of length 100 is needed here due to SQLITE_MAX_COMPOUND_SELECT
        # and SQLITE_LIMIT_VARIABLE_NUMBER under Windows.
        # See: https://github.com/coleifer/peewee/issues/948
        for chunk in chunks(events_dictlist, 100):
            EventModel.insert_many(chunk).execute()

    def _get_event(self, bucket_id, event_id) -> Optional[EventModel]:
        try:
            return (
                EventModel.select()
                .where(EventModel.id == event_id)
                .where(EventModel.bucket == self.bucket_keys[bucket_id])
                .get()
            )
        except peewee.DoesNotExist:
            return None

    def _get_last(self, bucket_id) -> EventModel:
        return (
            EventModel.select()
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .order_by(EventModel.timestamp.desc())
            .get()
        )

    def replace_last(self, bucket_id, event):
        e = self._get_last(bucket_id)
        e.timestamp = event.timestamp
        e.duration = f"S {event.duration.total_seconds()}"
        e.datastr = json.dumps(event.data)
        e.save()
        event.id = e.id
        return event

    def delete(self, bucket_id, event_id):
        return (
            EventModel.delete()
            .where(EventModel.id == event_id)
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .execute()
        )

    def replace(self, bucket_id, event_id, event):
        e = self._get_event(bucket_id, event_id)
        e.timestamp = event.timestamp
        e.duration = f"S {event.duration.total_seconds()}"
        e.datastr = json.dumps(event.data)
        e.save()
        event.id = e.id
        return event

    def get_event(
        self,
        bucket_id: str,
        event_id: int,
    ) -> Optional[Event]:
        """
        Fetch a single event from a bucket.
        """
        res = self._get_event(bucket_id, event_id)
        return Event(**EventModel.json(res)) if res else None

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        """
        Fetch events from a certain bucket, optionally from a given range of time.

        Example raw query:

            SELECT strftime(
              "%Y-%m-%d %H:%M:%f+00:00",
              ((to_date(timestamp - 2440587.5) * 86400),
              'unixepoch'
            )
            FROM eventmodel
            WHERE eventmodel.timestamp > '2021-06-20'
            LIMIT 10;

        """
        if limit == 0:
            return []
        

        q = (
            EventModel.select()
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .order_by(EventModel.timestamp.desc())
        )

        if limit > 0:
            q = q.limit(limit)

        q = self._where_range(q, starttime, endtime)
        res = q.execute()
        events = [Event(**e) for e in list(map(EventModel.json, res))]

        # Trim events that are out of range (as done in aw-server-rust)
        # TODO: Do the same for the other storage methods
        if starttime:
            starttime = starttime.astimezone(timezone.utc)
        if endtime:
            endtime = endtime.astimezone(timezone.utc)
        for e in events:
            if starttime:
                if e.timestamp < starttime:
                    e_end = e.timestamp + e.duration
                    e.timestamp = starttime
                    e.duration = e_end - e.timestamp
            if endtime:
                if e.timestamp + e.duration > endtime:
                    e.duration = endtime - e.timestamp

        return events

    def get_last_event(self, bucket_id, day=datetime.now()):
        event = (
            EventModel.select()
            .where((EventModel.bucket == self.bucket_keys[bucket_id]) & (peewee.fn.date_trunc('day', EventModel.timestamp) == day))
            .order_by(EventModel.timestamp.desc())
            .limit(1)
        )
        return event

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> int:
        q = EventModel.select().where(EventModel.bucket == self.bucket_keys[bucket_id])
        q = self._where_range(q, starttime, endtime)
        return q.count()

    def _where_range(
        self,
        q,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        # Important to normalize datetimes to UTC, otherwise any UTC offset will be ignored
        if starttime:
            starttime = starttime.astimezone(timezone.utc)
        if endtime:
            endtime = endtime.astimezone(timezone.utc)

        if starttime:
            # Faster WHERE to speed up slow query below, leads to ~2-3x speedup
            # We'll assume events aren't >24h
            q = q.where(starttime - timedelta(hours=24) <= EventModel.timestamp)

            # This can be slow on large databases...
            # Tried creating various indexes and using SQLite's unlikely() function, but it had no effect
            q = q.where(
                starttime <= EventModel.timestamp + EventModel.duration
            )
        if endtime:
            q = q.where(EventModel.timestamp <= endtime)

        return q
    
    def _get_user_by_mezon_user_id(self, mezon_user_id) -> Optional[UserModel]:
        try:
            return (
                UserModel.select()
                .where(UserModel.mezon_user_id == mezon_user_id)
                .get()
            )
        except peewee.DoesNotExist:
            return None

    def _get_user_by_email(self, email) -> Optional[UserModel]:
        try:
            return (
                UserModel.select()
                .where(UserModel.email == email)
                .get()
            )
        except peewee.DoesNotExist:
            return None

    def save_user(self, user_data):
        UserModel.delete().where(UserModel.email == user_data['email']).execute()

        last_used_at = datetime.now(timezone.utc)
        user = UserModel.create(
            device_id=user_data["device_id"],
            name=user_data["name"],
            email=user_data['email'],
            access_token=user_data["access_token"], 
            refresh_token=user_data["refresh_token"],
            mezon_user_id=user_data["mezon_user_id"] if "mezon_user_id" in user_data else None,
            last_used_at=last_used_at
        )
        user_data["last_used_at"] = last_used_at.isoformat()
        return user_data

    def get_user(self, filter):
        user = self._get_user_by_mezon_user_id(filter["mezon_user_id"])
        if user:
            return self._get_user_by_mezon_user_id(filter["mezon_user_id"]).json()
        return json.dumps({})
    
    def get_all_users(self):
        users = UserModel.select()
        result = []
        for user in users:
            result.append(user.json())
        return result

    def get_use_tracker(self, day=datetime.now().date()):
        users = UserModel.select().where(peewee.fn.date_trunc('day', UserModel.last_used_at) >= day)
        result = []
        for user in users:
            result.append(user.json())
        return result

    def save_report(self, report_data):
        ReportModel.create(
            email = report_data["email"],
            spent_time = report_data["spent_time"],
            call_time = report_data["call_time"],
            date = report_data["date"],
            wfh = report_data["wfh"],
        )
        return report_data

    def get_report(self, email, day=datetime.now()):
        try:
            report = ReportModel.select().where((ReportModel.email == email) & (peewee.fn.date_trunc('day', ReportModel.date) == day)).get().json()
            report['active_time'] = report['spent_time'] + report['call_time']
            return report
        except peewee.DoesNotExist:
            return None