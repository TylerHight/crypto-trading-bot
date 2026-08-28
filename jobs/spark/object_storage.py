from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession

from jobs.spark.curation import canonical_json_bytes


@dataclass(frozen=True)
class FileMetrics:
    files: int
    bytes: int


class HadoopObjectStore:
    """Small Hadoop-backed object API shared by local files and S3A paths."""

    def __init__(self, spark: SparkSession) -> None:
        jvm = spark.sparkContext._jvm
        if jvm is None:
            raise RuntimeError("Spark JVM is unavailable")
        self._jvm = jvm
        self._configuration = spark.sparkContext._jsc.hadoopConfiguration()

    def _path(self, uri: str) -> Any:
        return self._jvm.org.apache.hadoop.fs.Path(uri)

    def _filesystem(self, uri: str) -> Any:
        return self._path(uri).getFileSystem(self._configuration)

    def exists(self, uri: str) -> bool:
        return bool(self._filesystem(uri).exists(self._path(uri)))

    def read_bytes(self, uri: str) -> bytes:
        stream = self._filesystem(uri).open(self._path(uri))
        try:
            return bytes(stream.readAllBytes())
        finally:
            stream.close()

    def write_json_append_only(self, uri: str, value: dict[str, Any]) -> None:
        body = canonical_json_bytes(value)
        filesystem = self._filesystem(uri)
        path = self._path(uri)
        parent = path.getParent()
        if parent is not None:
            filesystem.mkdirs(parent)
        stream = filesystem.create(path, False)
        try:
            # canonical JSON uses ASCII-safe defaults, so writeBytes preserves
            # its exact digest across Hadoop implementations.
            stream.writeBytes(body.decode("ascii"))
            stream.hflush()
        finally:
            stream.close()

    def parquet_metrics(self, uri: str) -> FileMetrics:
        filesystem = self._filesystem(uri)
        iterator = filesystem.listFiles(self._path(uri), True)
        files = 0
        size = 0
        while iterator.hasNext():
            status = iterator.next()
            if str(status.getPath()).endswith(".parquet"):
                files += 1
                size += int(status.getLen())
        return FileMetrics(files, size)
