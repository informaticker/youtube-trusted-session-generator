import asyncio
import dataclasses
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp
from typing import Optional, Dict, Any, List, Callable, Awaitable

from playwright.async_api import async_playwright, Page, BrowserContext, Browser, Request

logger = logging.getLogger('extractor')


@dataclass
class TokenInfo:
    updated: int
    potoken: str
    visitor_data: str

    def to_json(self) -> str:
        as_dict = dataclasses.asdict(self)
        as_json = json.dumps(as_dict)
        return as_json


TokenUpdateCallback = Callable[[TokenInfo], Awaitable[None]]


class PotokenExtractor:

    def __init__(self, loop: asyncio.AbstractEventLoop,
                 update_interval: float = 3600,
                 browser_path: Optional[Path] = None) -> None:
        self.update_interval: float = update_interval
        self.browser_path: Optional[Path] = browser_path
        self.profile_path = mkdtemp()
        self._loop = loop
        self._token_info: Optional[TokenInfo] = None
        self._ongoing_update: asyncio.Lock = asyncio.Lock()
        self._extraction_done: asyncio.Event = asyncio.Event()
        self._update_requested: asyncio.Event = asyncio.Event()
        self._playwright = None
        self._browser = None
        self._token_update_callbacks: List[TokenUpdateCallback] = []

    def get(self) -> Optional[TokenInfo]:
        return self._token_info

    async def run_once(self) -> Optional[TokenInfo]:
        await self._update()
        return self.get()

    async def run(self) -> None:
        await self._update()
        while True:
            try:
                await asyncio.wait_for(self._update_requested.wait(), timeout=self.update_interval)
                logger.debug('initiating force update')
            except asyncio.TimeoutError:
                logger.debug('initiating scheduled update')
            await self._update()
            self._update_requested.clear()

    def request_update(self) -> bool:
        """Request immediate update, return False if update request is already set"""
        if self._ongoing_update.locked():
            logger.debug('update process is already running')
            return False
        if self._update_requested.is_set():
            logger.debug('force update has already been requested')
            return False
        self._loop.call_soon_threadsafe(self._update_requested.set)
        logger.debug('force update requested')
        return True

    async def register_token_update_callback(self, callback: TokenUpdateCallback) -> None:
        """Register a callback to be called when token is updated"""
        self._token_update_callbacks.append(callback)
        logger.debug(f'Registered new token update callback, total: {len(self._token_update_callbacks)}')


    async def unregister_token_update_callback(self, callback: TokenUpdateCallback) -> bool:
        """
        Unregister a previously registered callback
        Returns True if callback was found and removed, False otherwise
        """
        if callback in self._token_update_callbacks:
            self._token_update_callbacks.remove(callback)
            logger.debug(f'Unregistered token update callback, remaining: {len(self._token_update_callbacks)}')
            return True
        return False


    async def _notify_token_updated(self, token_info: TokenInfo) -> None:
        """Notify all registered callbacks about token update"""
        if not self._token_update_callbacks:
            return

        logger.debug(f'Notifying {len(self._token_update_callbacks)} callbacks about token update')

        tasks = []
        for callback in self._token_update_callbacks:
            tasks.append(asyncio.create_task(callback(token_info)))

        if tasks:
            try:
                await asyncio.gather(*tasks)
            except Exception as e:
                logger.error(f"Error in token update callback: {e}")

    def _extract_token(self, request_data: Dict[str, Any]) -> Optional[TokenInfo]:
        try:
            visitor_data = request_data['context']['client']['visitorData']
            potoken = request_data['serviceIntegrityDimensions']['poToken']
        except (TypeError, KeyError) as e:
            logger.warning(f'failed to extract token from request: {type(e)}, {e}')
            return None

        token_info = TokenInfo(
            updated=int(time.time()),
            potoken=potoken,
            visitor_data=visitor_data
        )
        return token_info

    async def _update(self) -> None:
        try:
            await asyncio.wait_for(self._perform_update(), timeout=600)
        except asyncio.TimeoutError:
            logger.error('update failed: hard limit timeout exceeded. Browser might be failing to start properly')

    async def _perform_update(self) -> None:
        if self._ongoing_update.locked():
            logger.debug('update is already in progress')
            return

        async with self._ongoing_update:
            logger.info('update started')
            self._extraction_done.clear()
            old_token = self._token_info

            try:
                if self._playwright is None:
                    self._playwright = await async_playwright().start()

                browser_type = self._playwright.chromium
                browser_args = {}

                if self.browser_path:
                    browser_args["executable_path"] = str(self.browser_path)

                context = await browser_type.launch_persistent_context(
                    user_data_dir=self.profile_path,
                    headless=False,
                    **browser_args
                )

                page = await context.new_page()

                await self._setup_request_interception(page)

                await page.goto('https://www.youtube.com/embed/jNQXAC9IVRw')

                player_clicked = await self._click_on_player(page)

                if player_clicked:
                    extraction_success = await self._wait_for_handler()
                    if not extraction_success:
                        logger.warning("No token was extracted in the allotted time")

                await page.close()
                await context.close()

                if self._token_info != old_token and self._token_info is not None:
                    await self._notify_token_updated(self._token_info)

            except Exception as e:
                logger.error(f"Error during token extraction: {type(e).__name__}: {e}")
                raise

    async def _setup_request_interception(self, page: Page) -> None:
        async def handle_request(request: Request):
            if request.method == 'POST' and '/youtubei/v1/player' in request.url:
                try:
                    post_data_str = request.post_data
                    if not post_data_str:
                        return

                    post_data = json.loads(post_data_str)
                    token_info = self._extract_token(post_data)

                    if token_info:
                        logger.info(f'new token: {token_info.to_json()}')
                        old_token = self._token_info
                        self._token_info = token_info
                        self._extraction_done.set()

                        if old_token != token_info:
                            asyncio.create_task(self._notify_token_updated(token_info))

                except Exception as e:
                    logger.warning(f"Error processing request: {e}")

        page.on('request', handle_request)

    async def _click_on_player(self, page: Page) -> bool:
        try:
            player = await page.wait_for_selector('#movie_player', timeout=10000)
            if player:
                await player.click()
                return True
            return False
        except Exception as e:
            logger.warning(f'update failed: unable to locate video player on the page: {e}')
            return False

    async def _wait_for_handler(self) -> bool:
        try:
            await asyncio.wait_for(self._extraction_done.wait(), timeout=30)
        except asyncio.TimeoutError:
            logger.warning('update failed: timeout waiting for outgoing API request')
            return False
        else:
            logger.info('update was successful')
            return True

    async def cleanup(self):
        """Close resources when the application exits"""
        if self._playwright:
            await self._playwright.stop()
