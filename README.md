<div align="center">
  <h1>kaflow</h1>
  <p>
    <em>Kafka streams topic processing in Python.</em>
  </p>
  <p>
    <a href="https://github.com/gabrielmbmb/kaflow/actions?query=workflow%3ATest+event%3Apush+branch%3Amain" target="_blank">
      <img src="https://github.com/gabrielmbmb/kaflow/workflows/Test/badge.svg?event=push&branch=main" alt="Test">
    </a>
    <a href="https://pypi.org/project/kaflow">
      <img src="https://img.shields.io/pypi/v/kaflow?color=#2cbe4e">
    </a>
    <a href="https://pypi.org/project/kaflow">
      <img src="https://img.shields.io/pypi/pyversions/kaflow?color=#2cbe4e">
    </a>
  </p>
</div>

---

`kaflow` is a simple framework that allows you to build Kafka streams processing aplications in Python with ease.

Some of the features offered by `kaflow`:

- Dependency Injection system inspired by [FastAPI](https://github.com/tiangolo/fastapi) and [xpresso](https://github.com/adriangb/xpresso), and backed by [di](https://github.com/adriangb/di).
- Automatic deserialization of incoming messages and serialization of outgoing messages. Supports popular formats like `JSON`, `Avro` or `Protobuf`.
- Message validation thanks to [pydantic](https://github.com/pydantic/pydantic).

## Requirements

Python 3.8+

## Installation

```shell
pip install kaflow
```

## Example

```python
from kaflow import Json, Kaflow
from pydantic import BaseModel


class UserClick(BaseModel):
    user_id: int
    url: str
    timestamp: int


app = Kaflow(name="AwesomeKakfaApp", brokers="localhost:9092")


@app.consume(topic="user_clicks", sink_topics=("user_clicks_json",))
async def consume_user_clicks(message: Json[UserClick]) -> Json[UserClick]:
    print("user click", message)
    return message


app.run()
```
