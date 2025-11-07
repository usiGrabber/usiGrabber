from enum import Enum

from usigrabber.backends.pride import PrideBackend


class BackendEnum(Enum):
    PRIDE = PrideBackend
