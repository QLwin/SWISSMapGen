import io
import os
import shutil
import uuid
import threading
import concurrent.futures
import time
import math
from collections import deque
from datetime import datetime
import requests
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from shapely.geometry import shape
from shapely.ops import transform as shapely_transform
from pyproj import Transformer
from werkzeug.exceptions import HTTPException

from config import MAX_DOWNLOAD_WORKERS, MAX_PIXELS, COLLECTION_RESOLUTIONS

# --- Global State ---
TASK_QUEUE = deque()
TASK_STATUS = {}
QUEUE_LOCK = threading.Lock()

# --- Helper Functions ---
def update_subtask_status(task_id, subtask, status_msg):
    """Helper function to update sub-task status safely."""
    with QUEUE_LOCK:
        if task_id in TASK_STATUS:
            TASK_STATUS[task_id]['progress'][subtask]['message'] = status_msg

def find_valid_tile_url(session, tile_name):
    """
    Iterates from the current year down to 2017 to find a valid URL for a single tile.
    """
    current_year = datetime.now().year
    for year in range(current_year, 2016, -1):
        base_url = f"https://data.geo.admin.ch/ch.swisstopo.swissimage-dop10/swissimage-dop10_{year}"
        url = f"{base_url}_{tile_name}/swissimage-dop10_{year}_{tile_name}_0.1_2056.tif"
        try:
            response = session.head(url, timeout=10)
            if response.status_code == 200:
                return url
        except requests.exceptions.RequestException:
            continue
    return None

def get_cog_urls_by_iteration(geom_lv95_shape, task_id, data_type, subtask_key):
    """
    Calculates required tiles and finds their valid URLs by iterating through years.
    """
    update_subtask_status(task_id, subtask_key, f"Calculating required {data_type} tiles...")
    minx, miny, maxx, maxy = geom_lv95_shape.bounds
    grid_size = 1000

    min_x_idx = math.floor(minx / grid_size)
    max_x_idx = math.floor(maxx / grid_size)
    min_y_idx = math.floor(miny / grid_size)
    max_y_idx = math.floor(maxy / grid_size)

    tile_names_to_check = []
    for x_idx in range(min_x_idx, max_x_idx + 1):
        for y_idx in range(min_y_idx, max_y_idx + 1):
            tile_names_to_check.append(f"{x_idx}-{y_idx}")
    
    valid_urls = []
    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
            future_to_tile = {executor.submit(find_valid_tile_url, session, name): name for name in tile_names_to_check}
            for i, future in enumerate(concurrent.futures.as_completed(future_to_tile)):
                with QUEUE_LOCK:
                    if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled':
                        return None 
                update_subtask_status(task_id, subtask_key, f"Validating tile URLs ({i + 1}/{len(tile_names_to_check)})...")
                url = future.result()
                if url:
                    valid_urls.append(url)
    
    return valid_urls

def process_dataset_remotely(geom_lv95_shape, task_id, data_type, subtask_key):
    """
    High-performance processing function that clips data directly from remote COG URLs
    at their native, full resolution.
    """
    urls = get_cog_urls_by_iteration(geom_lv95_shape, task_id, data_type, subtask_key)
    
    with QUEUE_LOCK:
        if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled' or urls is None:
            return None

    if not urls:
        update_subtask_status(task_id, subtask_key, 'No valid tiles found.')
        return None

    target_resolution = COLLECTION_RESOLUTIONS.get('ch.swisstopo.swissimage-dop10')
    resampling_method = Resampling.cubic
    
    sources_for_merging = []
    
    for i, url in enumerate(urls):
        update_subtask_status(task_id, subtask_key, f"Reading {data_type} from cloud ({i + 1}/{len(urls)})...")
        try:
            with rasterio.open(url) as src:
                vrt_options = {
                    'resampling': resampling_method, 'crs': src.crs,
                    'width': src.width, 'height': src.height,
                    'transform': src.transform
                }
                with WarpedVRT(src, **vrt_options) as vrt:
                    out_image, out_transform = mask(vrt, [geom_lv95_shape], crop=True)
                    
                    if out_image.any():
                        out_meta = src.meta.copy()
                        out_meta.update({
                            "driver": "GTiff", "height": out_image.shape[1],
                            "width": out_image.shape[2], "transform": out_transform
                        })
                        
                        memfile = io.BytesIO()
                        with rasterio.open(memfile, 'w', **out_meta) as dst:
                            dst.write(out_image)
                        memfile.seek(0)
                        sources_for_merging.append(rasterio.open(memfile))

        except Exception as e:
            print(f"Failed to process from URL: {url}, Error: {e}")
            continue

    if not sources_for_merging:
        update_subtask_status(task_id, subtask_key, 'Could not clip data from any valid source.')
        return None

    update_subtask_status(task_id, subtask_key, f"Merging clipped {data_type} fragments...")
    
    minx, miny, maxx, maxy = geom_lv95_shape.bounds
    out_width_px = (maxx - minx) / target_resolution[0]
    out_height_px = (maxy - miny) / target_resolution[1]

    effective_resolution = target_resolution
    if max(out_width_px, out_height_px) > MAX_PIXELS:
        scale_factor = max(out_width_px, out_height_px) / MAX_PIXELS
        effective_resolution = (target_resolution[0] * scale_factor, target_resolution[1] * scale_factor)
        update_subtask_status(task_id, subtask_key, f"Area too large, downsampling...")

    mosaic, out_trans = merge(sources_for_merging, res=effective_resolution, resampling=resampling_method)
    
    final_meta = sources_for_merging[0].meta.copy()
    final_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "transform": out_trans})

    for src in sources_for_merging:
        src.close()

    final_mem_file = io.BytesIO()
    with rasterio.open(final_mem_file, 'w', **final_meta) as dst:
        dst.write(mosaic)
    final_mem_file.seek(0)

    update_subtask_status(task_id, subtask_key, 'Processing complete')
    return final_mem_file

# --- Worker Thread ---
def worker():
    print(f"Worker thread started (PID: {os.getpid()})")
    while True:
        task_id, geometry = None, None
        with QUEUE_LOCK:
            if TASK_QUEUE:
                task_id, geometry = TASK_QUEUE.popleft()
        if task_id:
            with QUEUE_LOCK:
                if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled':
                    print(f"Task {task_id} was cancelled. Discarding from worker.")
                    continue

            print(f"Processing task: {task_id}")
            try:
                with QUEUE_LOCK:
                    TASK_STATUS[task_id]['status'] = 'processing'
                    TASK_STATUS[task_id]['message'] = 'Transforming coordinates...'
                transformer = Transformer.from_crs("EPSG:4326", "EPSG:2056", always_xy=True)
                geom_lv95_shape = shapely_transform(transformer.transform, shape(geometry))
                
                with QUEUE_LOCK:
                    TASK_STATUS[task_id]['message'] = 'Processing imagery...'
                    TASK_STATUS[task_id]['progress']['image']['status'] = 'processing'
                
                img_file = process_dataset_remotely(geom_lv95_shape, task_id, "imagery", "image")
                
                with QUEUE_LOCK:
                    if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled':
                        print(f"Task {task_id} was cancelled during processing. Halting.")
                        continue

                if not img_file:
                    raise ValueError("Imagery processing failed.")

                with QUEUE_LOCK:
                    TASK_STATUS[task_id]['message'] = 'Saving result file...'
                
                task_results_dir = os.path.join(os.getcwd(), 'results', task_id)
                os.makedirs(task_results_dir, exist_ok=True)
                
                dom_path = os.path.join(task_results_dir, 'dom_clipped.tif')
                with open(dom_path, 'wb') as f: f.write(img_file.getbuffer())
                download_urls = {'dom': f'/api/download/{task_id}/dom_clipped.tif'}

                with QUEUE_LOCK:
                    TASK_STATUS[task_id]['status'] = 'complete'
                    TASK_STATUS[task_id]['message'] = 'Processing complete! Ready for download.'
                    TASK_STATUS[task_id]['download_urls'] = download_urls
                print(f"Task {task_id} processed successfully.")

            except Exception as e:
                with QUEUE_LOCK:
                    if TASK_STATUS.get(task_id, {}).get('status') != 'cancelled':
                        print(f"Task {task_id} failed: {e}")
                        TASK_STATUS[task_id]['status'] = 'error'
                        TASK_STATUS[task_id]['message'] = f"Processing failed: {str(e)}"
        else:
            time.sleep(1)
