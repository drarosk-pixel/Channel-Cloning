# copied from https://github.com/tulir/mautrix-telegram/blob/master/mautrix_telegram/util/parallel_file_transfer.py
# Copyright (C) 2021 Tulir Asokan
import asyncio
import hashlib
import inspect
import logging
import math
import os
from collections import defaultdict
from typing import Optional, List, Union, DefaultDict, Tuple, BinaryIO

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
# Telegram rate-limits downloads and uploads separately, and an account can be fine with many
# upload connections while answering GetFileRequest with repeated short flood waits. Keeping a
# second knob means download concurrency can be lowered without slowing uploads down.
MAX_DL_CONNECTIONS = int(os.environ.get("FT_DL_CONNECTIONS", str(MAX_CONNECTIONS)))

# Telegram allows at most 512 KB per part for both uploads and downloads. Telethon otherwise
# picks 128 KB (files <=100 MB) or 256 KB (<=750 MB), which means 2-4x more round trips.
# Using the largest allowed part is a big win when the data centre is far away.
# Must stay a multiple of 4 KB so it is valid for downloads (4096) and uploads (1024).
_p = int(os.environ.get("FT_PART_KB", "512"))
PART_KB = max(4, min(512, _p - _p % 4))

_DC_NOTED = False      # so the data-centre note is printed once, not per file

TypeLocation = Union[Document, InputDocumentFileLocation, InputPeerPhotoFileLocation,
                     InputFileLocation, InputPhotoFileLocation]


class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[MTProtoSender]]
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

    async def init_senders(self, connections: int) -> List[MTProtoSender]:
        """Plain connections. Parts are handed out by a worker pool in both directions, so a
        slow or dying connection can only delay its own part.
        The first sender is made alone because a cross-datacentre one exports the
        authorization that the others then reuse."""
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

    async def run_pool(self, connections: int, part_count: int, do_part: callable,
                       on_error: list) -> None:
        """Hands out part numbers to the connections. Whoever is free takes the next one, so a
        connection that is retrying only delays its own part instead of the whole transfer."""
        queue: asyncio.Queue = asyncio.Queue()
        for i in range(part_count):
            queue.put_nowait(i)

        async def worker(slot: int):
            while not on_error:
                try:
                    index = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                for attempt in range(3):
                    try:
                        await do_part(slot, index)
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        if attempt == 2:
                            on_error.append(e)
                            return
                        print(f"Part {index} failed ({type(e).__name__}: {e}); "
                              f"reconnecting (attempt {attempt + 1}/3)", flush=True)
                        try:
                            await self.senders[slot].disconnect()
                        except Exception:
                            pass
                        try:
                            self.senders[slot] = await self._create_sender()
                        except Exception:
                            pass
                        # back off a little: retrying straight away only adds to the rate
                        # limit that caused the failure in the first place
                        await asyncio.sleep(2 * (attempt + 1))

        workers = [asyncio.ensure_future(worker(i)) for i in range(connections)]
        try:
            await asyncio.gather(*workers)
        finally:
            for w in workers:
                if not w.done():
                    w.cancel()
            # every task is awaited, so none is ever left running and unretrieved
            await asyncio.gather(*workers, return_exceptions=True)


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
    senders = await uploader.init_senders(connections)
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
    # Parallel download across data centres needs an exported authorization per connection,
    # which is the fragile part. Say so once, because it decides whether USE_FAST_DL helps.
    global _DC_NOTED
    if not _DC_NOTED:
        _DC_NOTED = True
        home = client.session.dc_id
        if dc_id != home:
            print(f"Note: files live in data centre {dc_id} but the account is on {home}. "
                  "Parallel download has to export an authorization per connection here; "
                  "if downloads keep failing, set USE_FAST_DL=0 (upload stays parallel).",
                  flush=True)
        else:
            print(f"Parallel download is same-datacentre (DC {home}).", flush=True)
    part_size = PART_KB * 1024
    part_count = max(1, math.ceil(size / part_size))

    # Lock per account: Telegram limits concurrent connections, so one parallel
    # transfer at a time per client avoids "server closed the connection" errors.
    async with parallel_transfer_locks[("dl", id(client))]:
        downloader = ParallelTransferrer(client, dc_id)
        connections = max(1, min(downloader._get_connection_count(size, MAX_DL_CONNECTIONS),
                                 part_count))
        await downloader.init_senders(connections)
        log.debug(f"Starting parallel download: {connections} {part_size} {part_count}")

        got = [0]
        errors = []
        write_lock = asyncio.Lock()

        async def fetch(slot: int, index: int):
            offset = index * part_size
            result = await client._call(downloader.senders[slot],
                                        GetFileRequest(location, offset=offset, limit=part_size))
            data = result.bytes
            # each part goes straight to its own place in the file, so parts can arrive in any
            # order and one slow connection never holds the others up
            async with write_lock:
                out.seek(offset)
                out.write(data)
                got[0] += len(data)

        async def ticker():
            while True:
                await asyncio.sleep(0.5)
                r = progress_callback(min(got[0], size), size)
                if inspect.isawaitable(r):
                    await r

        tick = asyncio.ensure_future(ticker()) if progress_callback else None
        try:
            await downloader.run_pool(connections, part_count, fetch, errors)
        finally:
            if tick is not None:
                tick.cancel()
            await downloader._cleanup()

        if errors:
            raise errors[0]
        if progress_callback:
            r = progress_callback(size, size)
            if inspect.isawaitable(r):
                await r
    return out


async def upload_file(client: TelegramClient,
                      file: BinaryIO,
                      progress_callback: callable = None,

                      ) -> TypeInputFile:
    # Lock per account to stay within Telegram's connection limits
    async with parallel_transfer_locks[("ul", id(client))]:
        res = (await _internal_transfer_to_telegram(client, file, progress_callback))[0]
    return res

