from __future__ import annotations

from asyncio import run

from primal_hunter.v2.get_toc import main as get_toc_main
from primal_hunter.v2.scrape_chapter import main as scrape_chapter_main

async def main() -> None:
    """Run the full Primal Hunter scraping pipeline."""
    get_toc_main()
    await scrape_chapter_main()

if __name__ == "__main__":
    run(main())

