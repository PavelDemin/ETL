import abc
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Save state to persistent storage"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Load state locally from persistent storage"""
        pass


class JsonFileStorage(BaseStorage):
    """
    Class for storing state in json file
    """
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """Save state to json file"""
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> dict:
        """Load state locally from json file"""
        with open(self.file_path, 'r') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                data = {}
        return data


class State:
    """
    Class for storing state when working with data
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Set state for a specific key"""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Get a state for a specific key"""
        data = self.storage.retrieve_state()
        return data.get(key, None)