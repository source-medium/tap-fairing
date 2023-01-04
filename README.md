# tap-fairing

`tap-fairing` is a Singer tap for fairing.co.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

### Implementation notes

In v0, only the `responses` api endpoint is replicated.
* `responses` are _always_ sorted like `ORDER BY inserted_at DESC`, which makes replication hard
* to get around this, the tap searches for the oldest record after `start_date` when run with no state, and paginates forward in time from there.
* the API URL params are somewhat confusing, but it helps to think about them like this:
  - `since` and `until` are filters that are applied to the data before pagination. They take dates and work as you expect
  - `after` and `before` are pagination params. They take an `id`. Since the results are sorted `DESC`, `after` means the next page of results and therefore returns _older_ records.
  - the `next` and `prev` links returned in the response payload use `after` and `before` respectively to get older and newer pages (`next` -> `after` -> older records)
  - annoyingly, the `next` and `prev` links are present in the response as long as there are records returned in the `data` list, and are `null` when no results are returned.
* the nitty gritty: if there's no `id` in the state, we do a binary search using `until` to find a partial page of records, which must include the oldest record. Starting from that page, we paginate forward using `before`. Each page is reversed before yielding records so they are in proper chronological order.
* the replication key is `id` because that's what we need to start forward pagination on incremental replication, so we need it in the state. We lie to the singer sdk and say records are sorted but please don't check.

## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/source-medium/tap-fairing.git@main
```

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| secret_token        | True     | None    | The token to authenticate against the fairing.co API |
| start_date          | False    | 2010-01-01T00:00:00Z | The earliest record date to sync |
| page_size           | False    |     100 | The page size for each responses endpoint call |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-fairing --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `tap-fairing` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-fairing --version
tap-fairing --help
tap-fairing --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_fairing/tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `tap-fairing` CLI interface directly using `poetry run`:

```bash
poetry run tap-fairing --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-fairing
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-fairing --version
# OR run a test `elt` pipeline:
meltano elt tap-fairing target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
