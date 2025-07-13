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

# --- WMTS Configuration ---
WMTS_BASE_URL = "https://wmts.geo.admin.ch/1.0.0"
WMTS_LAYER = "ch.swisstopo.swissimage-dop10"
WMTS_TILE_MATRIX_SET = "2056"
WMTS_TIME = "current"
WMTS_ZOOM_LEVEL = "22" # Corresponds to a resolution of ~0.1m/px, which is what we want
TILE_SIZE_PX = 256
LV95_ORIGIN = (2420000, 1350000)
RESOLUTIONS = { # Resolutions in meters per pixel for each zoom level
    "17": 2.5, "18": 2, "19": 1.5, "20": 1, "21": 0.5, "22": 0.25, "23": 0.1
}

# --- Helper Functions ---
def update_subtask_status(task_id, subtask, status_msg):
    """Helper function to update sub-task status safely."""
    with QUEUE_LOCK:
        if task_id in TASK_STATUS:
            TASK_STATUS[task_id]['progress'][subtask]['message'] = status_msg

def lv95_to_tile_coords(x, y, zoom):
    """Converts LV95 coordinates to WMTS tile column and row."""
    resolution = RESOLUTIONS[zoom]
    col = math.floor((x - LV95_ORIGIN[0]) / (TILE_SIZE_PX * resolution))
    row = math.floor((LV95_ORIGIN[1] - y) / (TILE_SIZE_PX * resolution))
    return col, row

def get_wmts_urls(geom_lv95_shape, task_id, subtask_key):
    """
    Calculates the required WMTS tile URLs for the given geometry.
    """
    update_subtask_status(task_id, subtask_key, "Calculating required imagery tiles...")
    minx, miny, maxx, maxy = geom_lv95_shape.bounds

    min_col, max_row = lv95_to_tile_coords(minx, miny, WMTS_ZOOM_LEVEL)
    max_col, min_row = lv95_to_tile_coords(maxx, maxy, WMTS_ZOOM_LEVEL)

    urls = []
    for r in range(min_row, max_row + 1):
        for c in range(min_col, max_col + 1):
            url = (
                f"{WMTS_BASE_URL}/{WMTS_LAYER}/default/{WMTS_TIME}/"
                f"{WMTS_TILE_MATRIX_SET}/{WMTS_ZOOM_LEVEL}/{r}/{c}.tif"
            )
            urls.append(url)
    
    update_subtask_status(task_id, subtask_key, f"Found {len(urls)} potential tiles. Validating...")

    # --- Validate URLs ---
    valid_urls = []
    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
            future_to_url = {executor.submit(session.head, url, timeout=10): url for url in urls}
            for i, future in enumerate(concurrent.futures.as_completed(future_to_url)):
                with QUEUE_LOCK:
                    if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled':
                        return None
                update_subtask_status(task_id, subtask_key, f"Validating tile URLs ({i + 1}/{len(urls)})...")
                try:
                    response = future.result()
                    if response.status_code == 200:
                        valid_urls.append(response.url)
                except requests.exceptions.RequestException:
                    continue # Ignore tiles that fail to resolve
    
    return valid_urls


def process_dataset_remotely(geom_lv95_shape, task_id, data_type, subtask_key):
    """
    High-performance processing function that clips data directly from remote WMTS tile URLs.
    """
    urls = get_wmts_urls(geom_lv95_shape, task_id, subtask_key)
    
    with QUEUE_LOCK:
        if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled' or urls is None:
            return None

    if not urls:
        update_subtask_status(task_id, subtask_key, 'No valid tiles found.')
        print(f"Task {task_id}: No valid tile URLs found for the given geometry.")
        return None

    print(f"Task {task_id}: Found {len(urls)} valid tile URLs. Starting processing.")
    target_resolution = RESOLUTIONS[WMTS_ZOOM_LEVEL]
    resampling_method = Resampling.cubic
    
    sources_for_merging = []
    
    for i, url in enumerate(urls):
        update_subtask_status(task_id, subtask_key, f"Reading {data_type} from cloud ({i + 1}/{len(urls)})...")
        try:
            with rasterio.open(url) as src:
                # Since we are getting tiles, we merge first, then mask.
                sources_for_merging.append(rasterio.open(url))
        except Exception as e:
            print(f"Task {task_id}: Failed to open URL: {url}, Error: {e}")
            continue

    if not sources_for_merging:
        update_subtask_status(task_id, subtask_key, 'Could not open any valid source for merging.')
        print(f"Task {task_id}: Could not create any data fragments from the sources.")
        return None

    update_subtask_status(task_id, subtask_key, f"Merging {len(sources_for_merging)} {data_type} fragments...")
    
    try:
        mosaic, out_trans = merge(sources_for_merging)
    finally:
        for src in sources_for_merging:
            src.close() # Ensure all source files are closed

    # Create an in-memory dataset from the merged mosaic
    out_meta = sources_for_merging[0].meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_trans,
    })

    with rasterio.io.MemoryFile() as memfile:
        with memfile.open(**out_meta) as dataset:
            dataset.write(mosaic)
            
            # Now, mask the merged dataset
            update_subtask_status(task_id, subtask_key, "Clipping merged data to selection...")
            clipped_image, clipped_transform = mask(dataset, [geom_lv95_shape], crop=True)

    if not clipped_image.any():
        update_subtask_status(task_id, subtask_key, 'Clipping resulted in an empty image.')
        return None

    final_meta = out_meta.copy()
    final_meta.update({
        "height": clipped_image.shape[1],
        "width": clipped_image.shape[2],
        "transform": clipped_transform,
    })

    final_mem_file = io.BytesIO()
    with rasterio.open(final_mem_file, 'w', **final_meta) as dst:
        dst.write(clipped_image)
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
                
                print(f"Attempting to save file for task {task_id}")
                task_results_dir = os.path.join(os.getcwd(), 'results', task_id)
                print(f"Task results directory: {task_results_dir}")
                os.makedirs(task_results_dir, exist_ok=True)
                print(f"Task results directory created/exists: {os.path.exists(task_results_dir)}")
                
                dom_path = os.path.join(task_results_dir, 'dom_clipped.tif')
                print(f"Output TIFF path: {dom_path}")
                
                try:
                    img_buffer = img_file.getbuffer()
                    print(f"Image buffer size: {len(img_buffer)} bytes")
                    with open(dom_path, 'wb') as f:
                        f.write(img_buffer)
                    print(f"File successfully written to {dom_path}")
                except Exception as write_e:
                    print(f"Error writing file {dom_path}: {write_e}")
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