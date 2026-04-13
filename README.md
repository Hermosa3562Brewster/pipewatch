# pipewatch

A lightweight CLI monitor for tracking the health and throughput of ETL pipelines in real time.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Start monitoring a pipeline by pointing pipewatch at your pipeline config or log source:

```bash
pipewatch monitor --config pipeline.yaml
```

Watch a specific pipeline stage and set a throughput alert threshold:

```bash
pipewatch monitor --stage transform --alert-threshold 500 --interval 5
```

Example output:

```
[pipewatch] 12:04:33 | Stage: transform | Status: ✓ healthy | Throughput: 1,240 rows/sec
[pipewatch] 12:04:38 | Stage: transform | Status: ✓ healthy | Throughput: 1,198 rows/sec
[pipewatch] 12:04:43 | Stage: transform | Status: ⚠ slow    | Throughput:   423 rows/sec
```

Run `pipewatch --help` for a full list of options.

---

## Configuration

pipewatch accepts a YAML config file to define pipeline stages, data sources, and alert rules:

```yaml
pipeline:
  name: my-etl
  stages:
    - extract
    - transform
    - load
alerts:
  throughput_min: 500
  notify: stderr
```

---

## CLI Options

| Option | Description | Default |
|---|---|---|
| `--config` | Path to pipeline YAML config file | — |
| `--stage` | Name of the pipeline stage to monitor | all stages |
| `--alert-threshold` | Minimum rows/sec before a slow warning is raised | `0` |
| `--interval` | Polling interval in seconds | `10` |
| `--output` | Output format: `text` or `json` | `text` |

---

## License

This project is licensed under the [MIT License](LICENSE).
