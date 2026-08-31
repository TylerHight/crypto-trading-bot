from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class StorageSettings:
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    region: str = "us-east-1"


@dataclass(frozen=True)
class StorageLocation:
    scheme: str
    bucket: str | None
    key: str


def parse_location(uri: str) -> StorageLocation:
    value = uri.strip()
    if not value:
        raise ValueError("storage location must not be empty")
    if len(value) >= 3 and value[1] == ":" and value[2] in {"/", "\\"}:
        return StorageLocation("file", None, value)
    parsed = urlparse(value)
    if parsed.scheme in {"s3", "s3a"}:
        if not parsed.netloc:
            raise ValueError("S3 location must include a bucket")
        return StorageLocation("s3", parsed.netloc, parsed.path.lstrip("/"))
    if parsed.scheme == "file":
        return StorageLocation("file", None, unquote(parsed.path))
    if parsed.scheme:
        raise ValueError(f"unsupported storage scheme: {parsed.scheme}")
    return StorageLocation("file", None, value)


def child_uri(base_uri: str, *parts: str) -> str:
    location = parse_location(base_uri)
    suffix = "/".join(part.strip("/") for part in parts if part.strip("/"))
    if location.scheme == "s3":
        key = "/".join(value for value in (location.key.rstrip("/"), suffix) if value)
        return f"s3a://{location.bucket}/{key}"
    return str(Path(location.key).joinpath(*parts))


class ObjectStorage:
    def __init__(self, settings: StorageSettings) -> None:
        self.settings = settings
        self._s3_client: Any | None = None

    def read_bytes(self, uri: str) -> bytes:
        location = parse_location(uri)
        if location.scheme == "file":
            return Path(location.key).read_bytes()
        response = self._s3().get_object(Bucket=location.bucket, Key=location.key)
        return response["Body"].read()

    def try_read_bytes(self, uri: str) -> bytes | None:
        try:
            return self.read_bytes(uri)
        except FileNotFoundError:
            return None
        except Exception as error:
            response = getattr(error, "response", None)
            code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
            status = (
                response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if isinstance(response, dict)
                else None
            )
            if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
                return None
            raise

    def write_bytes_append_only(self, uri: str, body: bytes, *, content_type: str) -> None:
        location = parse_location(uri)
        if location.scheme == "file":
            path = Path(location.key)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as output:
                output.write(body)
            return
        self._s3().put_object(
            Bucket=location.bucket,
            Key=location.key,
            Body=body,
            ContentType=content_type,
            IfNoneMatch="*",
        )

    def try_write_bytes_append_only(
        self, uri: str, body: bytes, *, content_type: str
    ) -> bool:
        try:
            self.write_bytes_append_only(uri, body, content_type=content_type)
        except FileExistsError:
            return False
        except Exception as error:
            response = getattr(error, "response", None)
            code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
            status = (
                response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if isinstance(response, dict)
                else None
            )
            if code in {"412", "PreconditionFailed"} or status == 412:
                return False
            raise
        return True

    def _s3(self) -> Any:
        if self._s3_client is None:
            import boto3  # type: ignore[import-untyped]

            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.settings.endpoint_url,
                aws_access_key_id=self.settings.access_key,
                aws_secret_access_key=self.settings.secret_key,
                region_name=self.settings.region,
            )
        return self._s3_client
