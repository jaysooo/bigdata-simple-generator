
from abc import ABC, abstractmethod

class Producer(ABC):

    @abstractmethod
    def produce(self, data: dict) -> None:
        pass

    @abstractmethod
    def build_dataset(self):
        pass

