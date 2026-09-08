from __future__ import annotations

import argparse

from .config import LOOPBACK_HOSTS, DashboardSettings
from .web import create_server


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the local read-only operator dashboard.")
    parser.add_argument("--host", choices=sorted(LOOPBACK_HOSTS))
    parser.add_argument("--port", type=int)
    return parser


def main() -> None:
    arguments = _parser().parse_args()
    settings = DashboardSettings.from_env()
    if arguments.host is not None or arguments.port is not None:
        settings = DashboardSettings(
            **{
                **settings.__dict__,
                "host": arguments.host or settings.host,
                "port": arguments.port if arguments.port is not None else settings.port,
            }
        )
    server = create_server(settings)
    host, port = server.server_address[:2]
    if isinstance(host, bytes):
        host = host.decode("ascii")
    print(f"Operator dashboard listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
