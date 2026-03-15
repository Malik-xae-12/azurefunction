from fastapi import FastAPI


def create_startup_handler(app: FastAPI):
    def start() -> None:
        return None

    return start


def create_shutdown_handler(app: FastAPI):
    def stop() -> None:
        return None

    return stop
