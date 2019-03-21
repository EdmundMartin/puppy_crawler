import asyncio
from typing import Union, List
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

from browser import BrowserRender, Response


class PageQueue:

    def __init__(self, seed_urls: Union[List[str], str]):
        self._page_queue = asyncio.Queue()
        self._seen_urls = set()
        self.seed_queue(seed_urls)
        self._active_jobs = 0

    def seed_queue(self, seed_urls: Union[List[str], str]):
        if isinstance(seed_urls, str):
            self._page_queue.put_nowait(seed_urls)
        elif isinstance(seed_urls, (set, list)):
            for item in seed_urls:
                self._page_queue.put_nowait(item)

    async def put_unique_url(self, url):
        if url not in self._seen_urls:
            await self._page_queue.put(url)
            self._seen_urls.add(url)

    async def get_next_url(self):
        while True:
            try:
                next_page = self._page_queue.get_nowait()
                self._active_jobs += 1
            except asyncio.QueueEmpty:
                if self._active_jobs > 0:
                    await asyncio.sleep(0.01)
                elif self._active_jobs <= 0:
                    raise asyncio.QueueEmpty("Queue empty with no pending jobs")
                else:
                    await asyncio.sleep(0.01)
            else:
                return next_page


class Crawler:

    def __init__(self, start_url: str):
        self.start_url = start_url
        self.base_host = '{parsed.scheme}://{parsed.netloc}'.format(parsed=urlparse(start_url))
        self.url_queue = PageQueue(self.start_url)
        self.loop = asyncio.get_event_loop()
        self.browser = BrowserRender(loop=self.loop, headless=False, tabs=5)

    async def urls_from_response(self, resp: Response):
        soup = BeautifulSoup(resp.html, 'lxml')
        links = soup.find_all('a', href=True)
        for l in links:
            tmp_link = l['href']
            joined = urljoin(self.start_url, tmp_link)
            if joined.startswith(self.base_host):
                await self.url_queue.put_unique_url(joined)
        return

    async def consume_queue(self, consume: int):
        while True:
            try:
                target = await self.url_queue.get_next_url()
                resp = await self.browser.get_request(target, timeout=30, post_load_wait=0)
                await self.urls_from_response(resp)
            except asyncio.QueueEmpty:
                return
            except Exception as e:
                print('Consumer {}'.format(consume), e)

    def run_scraper(self, workers: int) -> None:
        groups = [self.consume_queue(i) for i in range(workers)]
        work_group = asyncio.gather(*groups)

        loop = self.loop
        try:
            loop.run_until_complete(work_group)
        finally:
            loop.close()


if __name__ == '__main__':
    cr = Crawler('http://docs.pyexcel.org/en/latest/index.html')
    cr.run_scraper(2)