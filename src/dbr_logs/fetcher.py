import gzip
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from dbr_logs.databricks_client import DatabricksClient
from dbr_logs.models import LogFile, LogSource

MAX_WORKERS = 8


def fetch_sources(
    client: DatabricksClient,
    sources: list[LogSource],
    source_filter: str,
    stream_filter: str,
) -> dict[str, str]:
    pairs = _apply_source_filter(sources, source_filter, stream_filter)
    if not pairs:
        return {}

    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_file, client, log_file): log_file
            for _, log_file in pairs
        }
        for future in as_completed(futures):
            log_file = futures[future]
            try:
                path, content = future.result()
                results[path] = content
            except Exception as exc:
                print(
                    f"Warning: failed to fetch {log_file.path}: {exc}",
                    file=sys.stderr,
                )
    return results


def _fetch_file(client: DatabricksClient, log_file: LogFile) -> tuple[str, str]:
    raw_bytes = client.download_file(log_file.path)
    if log_file.is_compressed:
        text = gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
    else:
        text = raw_bytes.decode("utf-8", errors="replace")
    return (log_file.path, text)


def _apply_source_filter(
    sources: list[LogSource],
    source_filter: str,
    stream_filter: str,
) -> list[tuple[LogSource, LogFile]]:
    source_type, source_num = _parse_source_filter(source_filter)
    pairs: list[tuple[LogSource, LogFile]] = []

    for src in sources:
        if source_type != "all":
            if src.source_type != source_type:
                continue
            if source_num is not None and src.source_detail != source_num:
                continue

        for lf in src.files:
            if stream_filter != "all" and lf.stream != stream_filter:
                continue
            pairs.append((src, lf))

    return pairs


def _parse_source_filter(source: str) -> tuple[str, str | None]:
    if source.startswith("executor:"):
        return ("executor", source.split(":")[1])
    return (source, None)
