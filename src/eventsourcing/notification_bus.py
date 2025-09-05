from __future__ import annotations

import abc
import importlib
import logging

from eventsourcing.notifications import Notification

logger = logging.getLogger(__name__)


def get_notification_bus() -> "NotificationBus":
    return LocalNotificationBus()


class _Singleton:
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]


class NotificationBus(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def boot(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def dispatch(self, event: Notification):
        raise NotImplementedError()

    @abc.abstractmethod
    def dispatch_all(self, events: list[Notification]):
        raise NotImplementedError()


class LocalNotificationBus(_Singleton, NotificationBus):
    def __init__(self):
        self._booted = False
        self._event_subscriber_classes = {}

    def _boot_subscribers(self):
        from django.conf import settings

        subscribe_map = settings.BUSES_NOTIFICATION_SUBSCRIBERS

        for event_fqdn, subscriber_fqdns in subscribe_map.items():
            self._event_subscriber_classes[event_fqdn] = []

            for subscriber in subscriber_fqdns:
                module_name, class_name = subscriber.rsplit(".", 1)
                module = importlib.import_module(module_name)
                subscriber_class = getattr(module, class_name)

                self._event_subscriber_classes[event_fqdn].append(subscriber_class)

    def boot(self):
        if self._booted:
            return

        self._boot_subscribers()

        self._booted = True

    def dispatch(self, event: Notification):
        if not self._booted:
            self.boot()

        event_fqdn = ".".join([event.__class__.__module__, event.__class__.__qualname__])

        results = []

        for subscriber_class in self._event_subscriber_classes.get(event_fqdn, []):
            results.append(self._do_dispatch(subscriber_class, event))

    def dispatch_all(self, events: list[Notification]):
        for event in events:
            self.dispatch(event)

    def _do_dispatch(self, subscriber_class, event):
        instance = subscriber_class()
        instance.handle(event)
