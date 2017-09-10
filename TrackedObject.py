# coding=utf-8
"""
Provides TrackedObject class whose subclass are identified by ids.
For a given id, a call to the constructor always returns the same object.
Primarily meant for very light ORM-like libraries
"""
from threading import RLock
import weakref


class TrackedType(type):
    """
    Metaclass used for tracked objects
    """
    _mutex = RLock()

    def __new__(mcs, nom, bases, dct):
        mcs._mutex = RLock()
        if object not in bases:
            if "_load" not in dct:
                raise Exception('Subclasses of TrackedObject must implement a _load method')
            if "__new__" in dct:
                raise Exception('Subclasses of TrackedObject are not allowed to override __new__')
            if "id" in dct:
                raise Exception('Subclasses of TrackedObject are not allowed to override id')
            if "reload" not in dct:
                dct["reload"] = dct["_load"]
            if "__init__" in dct:
                dct["_tracked_init"] = dct["__init__"]
            dct["__init__"] = bases[0].__init__

        return type.__new__(mcs, nom, bases, dct)

    def __init__(cls, nom, bases, dct):
        if object not in bases:
            cls._instances = {}

        type.__init__(cls, nom, bases, dct)

    def __call__(cls, instance_id, *args, **kw):
        obj = cls.__new__(cls, instance_id, *args, **kw)
        if "force_reload" in kw:
            del kw["force_reload"]
        if getattr(obj, "_id", None) is None:
            try:
                obj.__init__(instance_id, *args, **kw)
            except:
                cls.clean(instance_id)
                raise
        return obj

    @staticmethod
    def lock_tracker():
        TrackedType._mutex.acquire()

    @staticmethod
    def release_tracker():
        TrackedType._mutex.release()

    def add(cls, instance_id, instance):
        cls._instances[instance_id] = weakref.ref(instance)

    def clean(cls, instance_id):
        del cls._instances[instance_id]

    def get(cls, instance_id):
        ref = cls._instances.get(instance_id)
        if not callable(ref):
            return None
        return ref()


class TrackedObject(object):
    """
    Abstract class
    Instances of TrackedObject subclasses are identified by an id,
    there will always be only one instance of this subclass with this id at all time
    Subclasses of TrackedObject:
        - must implement a _load method which receives at least an id
        - must __init__ is defined, __init__ and _load must have the same signatures
        - can implement a reload with a signature compatible with the one used for _load
        - if reload is not implemented, it will be an alias for _load
        - have an id() property that can be called to retrieve the id of the object
        - when creating an object, named parameter "force_reload" can be provided (will not be received by the init)
        - cannot overload __new__ or id
    """
    __metaclass__ = TrackedType

    def __init__(self, instance_id, *args, **kwargs):
        if getattr(self, "_id", None) is not None:
            return

        self._id = instance_id
        tracked_init = getattr(self, "_tracked_init", None)
        if callable(tracked_init):
            tracked_init(instance_id, *args, **kwargs)

        _load = getattr(self, "_load", None)
        _load(instance_id, *args, **kwargs)

    @property
    def id(self):
        """
        :return: the id provided at creation
        """
        return self._id

    def __new__(cls, instance_id, *args, **kwargs):
        TrackedType.lock_tracker()
        instance = cls.get(instance_id)
        if instance is not None:
            TrackedType.release_tracker()
            if kwargs.get("force_reload"):
                del kwargs["force_reload"]
                instance.reload(instance_id, *args, **kwargs)
        else:
            instance = super(TrackedObject, cls).__new__(cls)
            cls.add(instance_id, instance)
            TrackedType.release_tracker()

        return instance

__author__ = "Axel Faure"
__copyright__ = "Copyright 2017, Axel Faure"
__license__ = "Apache License 2.0"
__version__ = "1.0.0"
__maintainer__ = "Axel Faure"
