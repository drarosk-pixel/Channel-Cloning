# copied from https://github.com/tulir/mautrix-telegram/blob/master/mautrix_telegram/util/parallel_file_transfer.py
# Copyright (C) 2021 Tulir Asokan
import asyncio
import hashlib
import inspect
import logging
import math
import os
from collections import defaultdict
from typing import Optional, List, AsyncGenerator, Union, Awaitable, DefaultDict, Tuple, BinaryIO

from telethon import utils, helpers, TelegramClient
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import (GetFileRequest, SaveFilePartRequest,
                                          SaveBigFilePartRequest)
from telethon.tl.types import (Document, InputFileLocation, InputDocumentFileLocation,
                               InputPhotoFileLocation, InputPeerPhotoFileLocation, TypeInputFile,
                               InputFileBig, InputFile)

try:
    from mautrix.crypto.attachments import async_encrypt_attachment
except ImportError:
    async_encrypt_attachment = None

log: logging.Logger = logging.getLogger("telethon")

# Max parallel connections per transfer. Fewer = more stable on shared hosting
# (too many makes Telegram drop connections). Override with FT_CONNECTIONS.
MAX_CONNECTIONS = int(os.environ.get("FT_CONNECTIONS", "8"))

# Telegram allows at most 512 KB per part for both uploads and downloads. Telethon otherwise
# picks 128 KB (files <=100 MB) or 256 KB (<=750 MB), which means 2-4x more round trips.
# Using the largest allowed part is a big win when the data centre is far away.
# Must stay a multiple of 4 KB so it is valid for downloads (4096) and uploads (1024).
_p = int(os.environ.get("FT_PART_KB", "512"))
PART_KB = max(4, min(512, _p - _p % 4))

TypeLocation = Union[Document, InputDocumentFileLocation, InputPeerPhotoFileLocation,
                     InputFileLocation, InputPhotoFileLocation]


class DownloadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: GetFileRequest
    remaining: int
    stride: int

    def __init__(self, client: TelegramClient, sender: MTProtoSender, file: TypeLocation, offset: int, limit: int,
                 stride: int, count: int) -> None:
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    def disconnect(self) -> Awaitable[None]:
        return self.sender.disconnect()


class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[Union[DownloadSender, MTProtoSender]]]
    auth_key: AuthKey

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (None if dc_id and self.client.session.dc_id != dc_id
                         else self.client.session.auth_key)
        self.senders = None

    async def _cleanup(self) -> None:
        if not self.senders:
            return
        await asyncio.gather(*[sender.disconnect() for sender in self.senders],
                             return_exceptions=True)
        self.senders = None

    @staticmethod
    def _get_connection_count(file_size: int, max_count: int = MAX_CONNECTIONS,
                              full_size: int = 100 * 1024 * 1024) -> int:
        if file_size > full_size:
            return max_count
        return max(1, math.ceil((file_size / full_size) * max_count))

    async def _init_download(self, connections: int, file: TypeLocation, part_count: int,
                             part_size: int) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # The first cross-DC sender will export+import the authorization, so we always create it
        # before creating any other senders.
        self.senders = [
            await self._create_download_sender(file, 0, part_size, connections * part_size,
                                               get_part_count()),
            *await asyncio.gather(
                *[self._create_download_sender(file, i, part_size, connections * part_size,
                                               get_part_count())
                  for i in range(1, connections)])
        ]

    async def _create_download_sender(self, file: TypeLocation, index: int, part_size: int,
                                      stride: int,
                                      part_count: int) -> DownloadSender:
        return DownloadSender(self.client, await self._create_sender(), file, index * part_size, part_size,
                              stride, part_count)

    async def init_upload_senders(self, connections: int) -> List[MTProtoSender]:
        """Plain connections for uploading. Parts are handed out by a worker pool
        (see _internal_transfer_to_telegram) so a slow connection can't hold up the rest."""
        first = await self._create_sender()
        rest = (await asyncio.gather(*[self._create_sender() for _ in range(1, connections)])
                if connections > 1 else [])
        self.senders = [first, *rest]
        return self.senders

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(self.client._connection(dc.ip_address, dc.port, dc.id,
                                                     loggers=self.client._log,
                                                     proxy=self.client._proxy))
        if not self.auth_key:
            log.debug(f"Exporting auth to DC {self.dc_id}")
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(id=auth.id,
                                                                         bytes=auth.bytes)
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def download(self, file: TypeLocation, file_size: int,
                       part_size_kb: Optional[float] = None,
                       connection_count: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = int(part_size_kb or PART_KB) * 1024
        part_count = math.ceil(file_size / part_size)
        log.debug("Starting parallel download: "
                  f"{connection_count} {part_size} {part_count} {file!s}")
        await self._init_download(connection_count, file, part_count, part_size)

        # NOTE: cleanup is done by the caller (download_file), NOT in a finally here.
        # Awaiting inside an async generator's finally during GeneratorExit raises
        # "RuntimeError: coroutine ignored GeneratorExit", which could crash the whole copy.
        part = 0
        while part < part_count:
            tasks = [self.loop.create_task(sender.next()) for sender in self.senders]
            for task in tasks:
                data = await task
                if not data:
                    break
                yield data
                part += 1


parallel_transfer_locks: DefaultDict[int, asyncio.Lock] = defaultdict(lambda: asyncio.Lock())


def stream_file(file_to_stream: BinaryIO, chunk_size=1024):
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            break
        yield data_read


async def _internal_transfer_to_telegram(client: TelegramClient,
                                         response: BinaryIO,
                                         progress_callback: callable
                                         ) -> Tuple[TypeInputFile, int]:
    file_id = helpers.generate_random_long()
    file_size = os.path.getsize(response.name)

    hash_md5 = hashlib.md5()
    part_size = PART_KB * 1024
    part_count = (file_size + part_size - 1) // part_size
    is_large = file_size > 10 * 1024 * 1024

    uploader = ParallelTransferrer(client)
    connections = max(1, min(uploader._get_connection_count(file_size), part_count or 1))
    senders = await uploader.init_upload_senders(connections)
    log.debug(f"Starting parallel upload: {connections} {part_size} {part_count}")

    # Worker pool: every connection pulls the next part as soon as it is free. The previous
    # round-robin handed part N to connection N%count and waited for that one connection, so a
    # single slow connection left all the others idle (downloads never had this problem because
    # they fire every connection each round).
    queue: asyncio.Queue = asyncio.Queue(maxsize=connections * 2)
    sent = [0]      # bytes Telegram has confirmed
    errors = []

    def report(done: int):
        if not progress_callback:
            return None
        return progress_callback(min(done, file_size), file_size)

    async def worker(sender: MTProtoSender):
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                if errors:
                    continue        # keep draining so the producer never blocks
                index, data = item
                request = (SaveBigFilePartRequest(file_id, index, part_count, data) if is_large
                           else SaveFilePartRequest(file_id, index, data))
                try:
                    await client._call(sender, request)
                    sent[0] += len(data)
                except Exception as e:
                    errors.append(e)
            finally:
                queue.task_done()

    async def ticker():
        # keeps the reported speed smooth and truthful while the workers run
        while True:
            await asyncio.sleep(0.5)
            r = report(sent[0])
            if inspect.isawaitable(r):
                await r

    workers = [asyncio.ensure_future(worker(s)) for s in senders]
    tick = asyncio.ensure_future(ticker()) if progress_callback else None
    try:
        # read whole parts at a time (the original 1 KB reads meant ~1M Python loop
        # iterations per GB, which can slow uploads on a small VPS CPU)
        index = 0
        for data in stream_file(response, chunk_size=part_size):
            if errors:
                break
            if not is_large:
                hash_md5.update(data)
            await queue.put((index, data))
            index += 1
        for _ in senders:
            await queue.put(None)
        await asyncio.gather(*workers)
    finally:
        if tick is not None:
            tick.cancel()
        for w in workers:
            if not w.done():
                w.cancel()
        await uploader._cleanup()     # always release the parallel connections

    if errors:
        raise errors[0]
    r = report(file_size)
    if inspect.isawaitable(r):
        await r
    if is_large:
        return InputFileBig(file_id, part_count, "upload"), file_size
    else:
        return InputFile(file_id, part_count, "upload", hash_md5.hexdigest()), file_size


async def download_file(client: TelegramClient,
                        location: TypeLocation,
                        out: BinaryIO,
                        progress_callback: callable = None
                        ) -> BinaryIO:
    size = location.size
    dc_id, location = utils.get_input_location(location)
    # Lock per account: Telegram limits concurrent connections, so one parallel
    # transfer at a time per client avoids "server closed the connection" errors.
    async with parallel_transfer_locks[("dl", id(client))]:
        downloader = ParallelTransferrer(client, dc_id)
        try:
            async for x in downloader.download(location, size):
                out.write(x)
                if progress_callback:
                    r = progress_callback(out.tell(), size)
                    if inspect.isawaitable(r):
                        await r
        finally:
            # clean up the parallel connections here (outside the generator) so a half-consumed
            # download never raises "coroutine ignored GeneratorExit"
            await downloader._cleanup()
    return out


async def upload_file(client: TelegramClient,
                      file: BinaryIO,
                      progress_callback: callable = None,

                      ) -> TypeInputFile:
    # Lock per account to stay within Telegram's connection limits
    async with parallel_transfer_locks[("ul", id(client))]:
        res = (await _internal_transfer_to_telegram(client, file, progress_callback))[0]
    return res

