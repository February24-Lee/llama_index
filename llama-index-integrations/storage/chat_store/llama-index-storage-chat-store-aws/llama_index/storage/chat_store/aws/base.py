from typing import Any, List, Optional
from itertools import chain

import boto3
from boto3.dynamodb.conditions import Key

from llama_index.core.bridge.pydantic import Field, PrivateAttr
from llama_index.core.llms import ChatMessage
from llama_index.core.storage.chat_store.base import BaseChatStore
from llama_index.storage.chat_store.aws.utils import deserialize, serialize


from mypy_boto3_dynamodb import ServiceResource


DEFAULT_CHAT_TABLE = "ChatMessages"


class AWSDynamoChatStore(BaseChatStore):
    """AWS DynamoDB chat store."""

    _dynamo_client: ServiceResource = PrivateAttr()

    chat_table_name: str = Field(default=DEFAULT_CHAT_TABLE)
    chat_table_key_name: str = Field(default="PartitionKey")

    def __init__(
        self,
        dynamo_client: Any,
        chat_table_name: str = DEFAULT_CHAT_TABLE,
        chat_table_key_name: str = "PartitionKey",
    ):
        super().__init__(
            chat_table_name=chat_table_name,
        )
        self._dynamo_client = dynamo_client
        self.chat_table_key_name = chat_table_key_name

    @classmethod
    def from_aws_credentials(
        cls,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str,
        chat_table_name: str = DEFAULT_CHAT_TABLE,
        chat_table_key_name: str = "PartitionKey",
    ) -> "AWSDynamoChatStore":
        dynamo_client = boto3.resource(
            "dynamodb",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        return cls(
            dynamo_client=dynamo_client,
            chat_table_name=chat_table_name,
            chat_table_key_name=chat_table_key_name,
        )

    def set_messages(self, key: str, messages: List[ChatMessage]) -> None:
        """Set messages for a key."""
        chat_table = self._dynamo_client.Table(self.chat_table_name)

        # Delete existing messages and insert new messages
        response = chat_table.query(
            KeyConditionExpression=Key(self.chat_table_key_name).eq(key)
        )
        with chat_table.batch_writer() as batch:
            for item in response["Items"]:
                batch.delete_item(
                    Key={self.chat_table_key_name: key, "RowKey": item["RowKey"]}
                )
            for idx, message in enumerate(messages):
                batch.put_item(
                    Item={
                        self.chat_table_key_name: key,
                        "RowKey": self._to_row_key(idx),
                        "msg": serialize(message.dict()),
                    }
                )

    def get_messages(self, key: str) -> List[ChatMessage]:
        """Get messages for a key."""
        chat_table = self._dynamo_client.Table(self.chat_table_name)
        response = chat_table.query(
            KeyConditionExpression=Key(self.chat_table_key_name).eq(key)
        )
        return [
            ChatMessage.parse_obj(deserialize(item["msg"]))
            for item in response["Items"]
        ]

    def add_message(self, key: str, message: ChatMessage, idx: int = None):
        """Add a message for a key."""
        next_index = len(self.get_messages(key))
        if idx is not None and idx > next_index:
            raise ValueError(f"Index out of bounds: {idx}")
        elif idx is None:
            idx = next_index

        chat_table = self._dynamo_client.Table(self.chat_table_name)
        chat_table.put_item(
            Item={
                self.chat_table_key_name: key,
                "RowKey": self._to_row_key(idx),
                "msg": serialize(message.dict()),
            }
        )

    def delete_messages(self, key: str) -> Optional[List[ChatMessage]]:
        """Delete messages for a key."""
        chat_table = self._dynamo_client.Table(self.chat_table_name)

        # Delete messages
        response = chat_table.query(
            KeyConditionExpression=Key(self.chat_table_key_name).eq(key)
        )
        with chat_table.batch_writer() as batch:
            for item in response["Items"]:
                batch.delete_item(
                    Key={self.chat_table_key_name: key, "RowKey": item["RowKey"]}
                )

    def delete_message(self, key: str, idx: int) -> Optional[ChatMessage]:
        """Delete specific message for a key."""
        chat_table = self._dynamo_client.Table(self.chat_table_name)

        chat_table.delete_item(
            Key={self.chat_table_key_name: key, "RowKey": self._to_row_key(idx)}
        )

    def delete_last_message(self, key: str) -> Optional[ChatMessage]:
        """Delete last message for a key."""
        chat_table = self._dynamo_client.Table(self.chat_table_name)
        last_row_key = self._to_row_key(len(self.get_messages(key)) - 1)
        chat_table.delete_item(
            Key={self.chat_table_key_name: key, "RowKey": last_row_key}
        )

    def get_keys(self) -> List[str]:
        """Get all keys."""
        chat_table = self._dynamo_client.Table(self.chat_table_name)
        response = chat_table.scan()
        return set([item[self.chat_table_key_name] for item in response["Items"]])

    @classmethod
    def class_name(cls) -> str:
        """Get class name."""
        return "AWSDynamoChatStore"

    def _to_row_key(self, idx: int) -> str:
        """Generate a row key from an index."""
        return f"{idx:010}"
