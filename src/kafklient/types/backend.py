# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from confluent_kafka import (
    OFFSET_END,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, ClusterMetadata, NewTopic


def get_error_code(name: str) -> int | None:
    prop = getattr(KafkaError, name, None)
    if prop is None:
        return None
    return int(prop)


KAFKA_ERROR_PARTITION_EOF = get_error_code("_PARTITION_EOF")

__all__ = [
    "Consumer",
    "Producer",
    "KafkaError",
    "Message",
    "OFFSET_END",
    "TopicPartition",
    "ClusterMetadata",
    "KafkaException",
    "AdminClient",
    "NewTopic",
    "KAFKA_ERROR_PARTITION_EOF",
]
