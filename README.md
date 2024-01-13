# multiprocessing + sqlite demo

## Description

### Forward compatible, more secure multiprocessing

`mutiprocessing` defaults to `fork` context on POSIX (excluding macOS).
this demo uses `spawn` instead, as it's more secure and available on all
major platform.

### Serialized sqlite write

with `multiprocessing.Lock`, concurrent sqlite writes are serialized
and won't cause "Database is locked" errors.

each child process maintains its own `sqlalchemy.Engine`, since the
engine is not share-able.

### multiprocessing logger setup

this project also comes with a `QueueHandler` / `QueueListener` based,
multiprocessing capable logging setup.

from the main process:

* set up the basic logging via `dictConfig`
* setup a `multiprocessing.Queue` for logging
* when `_log_listener` is in effect, handlers for root logger
  is replaced with a `QueueHandler`, and actual logging is done
  via `QueueListener`

form the child process, in `worker.init`, `basicConfig` would
set up a `QueueHandler` which writes logs into parent-provided
`multiprocessing.Queue`.

## Requirements / Test setup

this demo is tested on Archlinux with python 3.11, and it's
dependency is managed by `poetry`.

For more detail about required packages, please refer to `pyproject.toml`

## License

The MIT License; see also `LICENSE.txt`
