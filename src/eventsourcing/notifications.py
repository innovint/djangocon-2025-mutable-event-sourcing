import abc


class Notification(metaclass=abc.ABCMeta):
    def dispatch(self):
        from eventsourcing.notification_bus import get_notification_bus

        bus = get_notification_bus()

        return bus.dispatch(self)


class Subscriber(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def handle(self, event: Notification):
        raise NotImplementedError()
