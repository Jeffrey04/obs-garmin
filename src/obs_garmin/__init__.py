import asyncio
import logging
import os
import signal
import sys
from concurrent.futures import Future, ProcessPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from multiprocessing import Manager
from queue import Empty, Queue
from struct import unpack
from threading import Event
from types import FrameType
from typing import Any, Callable, TextIO

import typer
from bleak import BleakClient, BleakScanner
from dotenv import load_dotenv
from obsws_python import ReqClient
from rich import print
from rich.prompt import IntPrompt, Prompt

app = typer.Typer()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig()


HR_MEASUREMENT_CHARACTERISTIC_UUID = "00002a37-0000-1000-8000-00805f9b34fb"

type OBS_CONFIG = tuple[str, str, str, str]


@dataclass
class ShutdownHandler:
    exit_event: Event

    def __call__(
        self, signum: int | None = None, frame: FrameType | None = None
    ) -> None:
        logger.info("MAIN: Sending exit event to all tasks in pool")
        self.exit_event.set()


@dataclass
class DoneHandler:
    name: str
    shutdown_handler: ShutdownHandler

    def __call__(self, future: Future) -> None:
        logger.info(
            "MAIN: Task is done, prompting others to quit. name=%s, future=%s",
            self.name,
            future,
        )

        if future.exception() is not None:
            logger.exception(future.exception())  # type: ignore

        self.shutdown_handler(None, None)


@dataclass
class HeartbeatHandler:
    queue: Queue

    def __call__(self, sender, data) -> None:
        """Callback to handle heart rate measurement data."""
        # The heart rate data structure is defined in the BLE spec:
        # Byte 0: Flags (contains format of HR value and if other fields are present)
        # Byte 1: Heart Rate Value (8-bit or 16-bit)

        # Check the first bit of the flags to see if HR is 16-bit (0x01) or 8-bit (0x00)
        flags = data[0]
        is_16_bit = flags & 0x01

        if is_16_bit:
            # 16-bit HR value is at index 1 and 2
            hr_value = unpack("<H", data[1:3])[0]
        else:
            # 8-bit HR value is at index 1
            hr_value = data[1]

        # You can add logic here for other fields like Sensor Contact, Energy Expended, etc.
        logger.info(f"[{flags:04b}] Heart Rate: {hr_value} BPM")
        asyncio.create_task(asyncio.to_thread(self.queue.put, hr_value))


@dataclass
class Submission:
    name: str
    callable: Callable[..., Any]
    queue: Queue
    exit_event: Event
    shutdown_handler: ShutdownHandler
    is_async: bool = True

    def submit(self, executor: ProcessPoolExecutor) -> Future:
        future = executor.submit(self)
        future.add_done_callback(DoneHandler(self.name, self.shutdown_handler))

        return future

    def __call__(self) -> None:
        if self.is_async:
            asyncio.run(self.callable(exit_event=self.exit_event, queue=self.queue))
        else:
            self.callable(exit_event=self.exit_event, queue=self.queue)


@app.command("setup")
def setup() -> None:
    async def _setup(file: TextIO) -> None:
        device = await setup_device()
        obs_host, obs_port, obs_password, obs_source = await setup_obs()

        file.write(f"DEVICE_ADDRESS={device}\n")
        file.write(f"OBS_HOST={obs_host}\n")
        file.write(f"OBS_PORT={obs_port}\n")
        file.write(f"OBS_PASSWORD={obs_password}\n")
        file.write(f"OBS_SOURCE={obs_source}")

    with open(".env", "w") as file:
        asyncio.run(_setup(file))


@app.command("run")
def run() -> None:
    load_dotenv()

    manager = Manager()
    queue = manager.Queue()
    event = manager.Event()

    shutdown_handler = ShutdownHandler(event)

    for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        signal.signal(s, shutdown_handler)

    with ProcessPoolExecutor(max_workers=5) as executor:
        try:
            Submission(
                "consumer", consumer, queue, event, shutdown_handler, False
            ).submit(executor)
            Submission("producer", producer, queue, event, shutdown_handler).submit(
                executor
            )
        except Exception as e:
            logger.exception(e)
            event.set()


@app.command("discover")
def discover() -> None:
    async def _discover():
        logger.info(
            f"Connecting to {os.environ['DEVICE_ADDRESS']} for service discovery..."
        )
        try:
            async with BleakClient(os.environ["DEVICE_ADDRESS"]) as client:
                if not client.is_connected:
                    print("Failed to connect for discovery.")
                    return

                logger.info("Successfully connected! Discovering services...")

                # The client automatically discovers services upon connection
                # We now iterate through them and print the structure
                for service in client.services:
                    logger.info(
                        f"\n[Service] UUID: {service.uuid} | Description: {service.description}"
                    )
                    for char in service.characteristics:
                        permissions = ", ".join(char.properties)
                        logger.info(
                            f"  [Characteristic] UUID: {char.uuid} | Handle: {char.handle} | Properties: [{permissions}]"
                        )
                        for descriptor in char.descriptors:
                            logger.info(
                                f"    [Descriptor] UUID: {descriptor.uuid} | Handle: {descriptor.handle}"
                            )

        except Exception as e:
            logger.error(f"An error occurred during discovery")
            logger.exception(e)

    load_dotenv()

    asyncio.run(_discover())


async def setup_device() -> str:
    ble_devices = await BleakScanner.discover(timeout=10.0)

    print(f"--- Discovered {len(ble_devices)} BLE Devices ---")
    for i, device in enumerate(ble_devices):
        print(
            f"#{i}: Name: {device.name}, Address: {device.address}, Details: {device}"
        )

    try:
        return ble_devices[
            int(
                await asyncio.to_thread(
                    IntPrompt.ask,
                    "Choose your device",
                    choices=tuple(str(x) for x in range(len(ble_devices))),
                )  # type: ignore
            )
        ].address

    except (ValueError, IndexError) as e:
        print("Invalid input, exiting")

        sys.exit()


async def setup_obs() -> OBS_CONFIG:
    return (
        await asyncio.to_thread(Prompt.ask, "OBS websocket host address"),
        str(await asyncio.to_thread(IntPrompt.ask, "OBS websocket port")),
        await asyncio.to_thread(Prompt.ask, "OBS websocket password", password=True),
        await asyncio.to_thread(Prompt.ask, "OBS websocket source"),
    )


async def producer(queue: Queue, exit_event: Event) -> None:
    logger.info(f"Attempting to connect to {os.environ['DEVICE_ADDRESS']} ...")

    try:
        async with BleakClient(
            os.environ["DEVICE_ADDRESS"],
            timeout=15.0,
            pair=True,
            disconnected_callback=ShutdownHandler(exit_event),  # type: ignore
        ) as client:
            logging.info("Connected! Subscribing to HR characteristic...")

            # This is the key step to start receiving data
            await client.start_notify(
                HR_MEASUREMENT_CHARACTERISTIC_UUID,
                HeartbeatHandler(queue),
            )

            # Keep the connection alive to receive notifications
            logger.info("Waiting for heart rate data...")

            await asyncio.to_thread(exit_event.wait)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.exception(e)

        exit_event.set()

    finally:
        logger.info("Client disconnected.")


def consumer(queue: Queue, exit_event: Event) -> None:
    obs = ReqClient(
        host=os.environ["OBS_HOST"],
        port=int(os.environ["OBS_PORT"]),
        password=os.environ["OBS_PASSWORD"],
    )

    try:
        logger.info("Starting consumption loop")
        while True:
            if exit_event.is_set():
                logger.info("Exiting consumption loop")
                break

            with suppress(Empty):
                if item := queue.get(timeout=5.0):
                    obs.set_input_settings(
                        os.environ["OBS_SOURCE"],
                        {"text": f"{item:3d}"},
                        True,
                    )
    except Exception as e:
        logger.exception(e)
        exit_event.set()