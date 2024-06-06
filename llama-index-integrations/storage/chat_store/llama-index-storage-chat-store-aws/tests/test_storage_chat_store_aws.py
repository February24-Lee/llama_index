from llama_index.core.storage.chat_store.base import BaseChatStore
from llama_index.storage.chat_store.aws import AWSDynamoChatStore


def test_class():
    names_of_base_classes = [b.__name__ for b in AWSDynamoChatStore.__mro__]
    assert BaseChatStore.__name__ in names_of_base_classes
