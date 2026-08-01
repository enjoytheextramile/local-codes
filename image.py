"""
Image Download & B&W Conversion Pipeline
==========================================
Downloads 1000+ images from an API and converts them to grayscale
using multithreading (I/O-bound downloads) and multiprocessing (CPU-bound conversion).

Usage:
    python image_pipeline.py
"""

import os
import time
import logging
import requests
from io import BytesIO
from dataclasses import dataclass, field
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from threading import Semaphore
from queue import Queue
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase 0 — Configuration (answers to all discovery questions in one place)
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    # API constraints
    api_base_url: str = "https://picsum.photos/id/{id}/800/600"
    image_ids: list = field(default_factory=lambda: list(range(1, 1001)))
    api_rate_limit: int = 10              # max requests per second
    api_timeout: int = 15                 # seconds per request
    max_retries: int = 3
    retry_backoff: float = 2.0            # exponential backoff multiplier

    # Image characteristics
    max_image_size_mb: int = 50           # skip images larger than this
    input_format: str = "JPEG"            # expected source format

    # Storage
    raw_dir: str = "images/raw"           # original downloads
    bw_dir: str = "images/bw"            # converted grayscale output
    keep_originals: bool = False          # delete raw after conversion

    # Output
    output_format: str = "JPEG"           # output file format
    output_quality: int = 85              # JPEG quality (1-100)

    # Infrastructure
    download_workers: int = 20            # threads for downloading
    convert_workers: Optional[int] = None # processes for conversion (None = cpu_count)
    convert_chunksize: int = 20           # batch size per process dispatch


# ---------------------------------------------------------------------------
# Phase 1 — Download images (I/O-bound → Threads)
# ---------------------------------------------------------------------------

class ImageDownloader:
    """Thread-based concurrent downloader with rate limiting and retries."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.semaphore = Semaphore(config.api_rate_limit)
        self.session = requests.Session()
        # connection pooling — reuse TCP connections
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=config.download_workers,
            pool_maxsize=config.download_workers,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _download_one(self, image_id: int) -> Optional[str]:
        """Download a single image with retry + rate limiting."""
        url = self.config.api_base_url.format(id=image_id)
        dest = os.path.join(self.config.raw_dir, f"image_{image_id}.jpg")

        # skip if already downloaded (resume support)
        if os.path.exists(dest):
            return dest

        for attempt in range(1, self.config.max_retries + 1):
            self.semaphore.acquire()
            try:
                resp = self.session.get(url, timeout=self.config.api_timeout)
                resp.raise_for_status()

                # validate size
                size_mb = len(resp.content) / (1024 * 1024)
                if size_mb > self.config.max_image_size_mb:
                    logger.warning(f"Skipping {url}: {size_mb:.1f}MB exceeds limit")
                    return None

                with open(dest, "wb") as f:
                    f.write(resp.content)
                return dest

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    wait = self.config.retry_backoff ** attempt
                    logger.warning(f"Rate limited on {url}, waiting {wait}s")
                    time.sleep(wait)
                elif attempt == self.config.max_retries:
                    logger.error(f"Failed after {attempt} attempts: {url} — {e}")
                    return None
            except requests.exceptions.RequestException as e:
                if attempt == self.config.max_retries:
                    logger.error(f"Failed after {attempt} attempts: {url} — {e}")
                    return None
                time.sleep(self.config.retry_backoff ** attempt)
            finally:
                self.semaphore.release()

        return None

    def download_all(self) -> list[str]:
        """Download all images concurrently using a thread pool."""
        os.makedirs(self.config.raw_dir, exist_ok=True)
        paths = []
        failed = 0

        logger.info(f"Downloading {len(self.config.image_ids)} images with {self.config.download_workers} threads")

        with ThreadPoolExecutor(max_workers=self.config.download_workers) as executor:
            futures = {
                executor.submit(self._download_one, img_id): img_id
                for img_id in self.config.image_ids
            }
            for future in as_completed(futures):
                result = future.result()
                if result:
                    paths.append(result)
                else:
                    failed += 1

        logger.info(f"Download complete: {len(paths)} succeeded, {failed} failed")
        return paths


# ---------------------------------------------------------------------------
# Phase 2 — Convert to B&W (CPU-bound → Processes)
# ---------------------------------------------------------------------------

def _convert_single(args: tuple[str, str, str, int]) -> Optional[str]:
    """
    Convert one image to grayscale. Runs in a worker process.
    Must be a top-level function (pickle-serializable for multiprocessing).
    """
    src_path, dst_dir, output_format, output_quality = args
    try:
        img = Image.open(src_path)
        bw = img.convert("L")  # 8-bit grayscale

        filename = os.path.basename(src_path)
        name, _ = os.path.splitext(filename)
        ext = "jpg" if output_format.upper() == "JPEG" else output_format.lower()
        dst_path = os.path.join(dst_dir, f"bw_{name}.{ext}")

        save_kwargs = {}
        if output_format.upper() == "JPEG":
            save_kwargs["quality"] = output_quality

        bw.save(dst_path, format=output_format, **save_kwargs)
        return dst_path

    except Exception as e:
        logger.error(f"Conversion failed for {src_path}: {e}")
        return None


class ImageConverter:
    """Process-based concurrent image converter."""

    def __init__(self, config: PipelineConfig):
        self.config = config

    def convert_all(self, paths: list[str]) -> list[str]:
        """Convert all images to grayscale using a process pool."""
        os.makedirs(self.config.bw_dir, exist_ok=True)

        args = [
            (p, self.config.bw_dir, self.config.output_format, self.config.output_quality)
            for p in paths
        ]
        results = []
        failed = 0
        workers = self.config.convert_workers or os.cpu_count()

        logger.info(f"Converting {len(paths)} images with {workers} processes")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            for result in executor.map(
                _convert_single, args, chunksize=self.config.convert_chunksize
            ):
                if result:
                    results.append(result)
                else:
                    failed += 1

        logger.info(f"Conversion complete: {len(results)} succeeded, {failed} failed")
        return results


# ---------------------------------------------------------------------------
# Phase 3 — Cleanup
# ---------------------------------------------------------------------------

def cleanup_originals(paths: list[str]):
    """Remove raw files after successful conversion."""
    for p in paths:
        try:
            os.remove(p)
        except OSError:
            pass
    logger.info(f"Cleaned up {len(paths)} original files")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(config: PipelineConfig = None):
    config = config or PipelineConfig()
    start = time.perf_counter()

    # Phase 1: Download (threads — I/O bound)
    downloader = ImageDownloader(config)
    raw_paths = downloader.download_all()

    if not raw_paths:
        logger.error("No images downloaded. Exiting.")
        return

    # Phase 2: Convert (processes — CPU bound)
    converter = ImageConverter(config)
    bw_paths = converter.convert_all(raw_paths)

    # Phase 3: Cleanup
    if not config.keep_originals:
        cleanup_originals(raw_paths)

    elapsed = time.perf_counter() - start
    logger.info(
        f"Pipeline complete: {len(bw_paths)} B&W images in {config.bw_dir} "
        f"({elapsed:.1f}s)"
    )


if __name__ == "__main__":
    run_pipeline()
