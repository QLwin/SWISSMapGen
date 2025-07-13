# ==============================================================================
# Geo-Processing Service with Task Queue for Swiss Topo Data
# ==============================================================================
#
# 功能说明:
# -----------------
# 本服务是一个实现了异步任务队列的Flask Web API。它能接收多个并发请求，
# 将它们放入一个处理队列中，并允许前端通过任务ID来轮询状态和下载最终结果。
# 1. 接收一个地理区域，为其生成一个唯一的任务ID，并将其加入队列。
# 2. 立即返回任务ID和队列位置给前端。
# 3. 后台有一个独立的工作线程，按顺序处理队列中的任务（下载、镶嵌、裁剪、压缩）。
# 4. 前端可以通过状态API查询任务进度。
# 5. 任务完成后，处理好的文件被保存，前端通过下载API获取结果。
#
# API调用方法:
# -----------------
# 1. 提交任务:
#   - 端点: /api/process_area
#   - 方法: POST
#   - 请求体: {"geometry": <GeoJSON_Polygon>}
#   - 成功响应: {"task_id": "...", "status": "queued", "position": 1}
#
# 2. 查询状态:
#   - 端点: /api/task_status/<task_id>
#   - 方法: GET
#   - 成功响应: {"status": "processing", "message": "正在处理DEM..."} 或
#             {"status": "complete", "download_url": "/api/download/<task_id>"}
#
# 3. 下载结果:
#   - 端点: /api/download/<task_id>
#   - 方法: GET
#   - 成功响应: 一个名为 'swisstopo_data.7z' 的二进制文件流。
#
# ==============================================================================
# ==============================================================================
# Geo-Processing Service with Task Queue for Swiss Topo Data
# ==============================================================================
# ==============================================================================
# Geo-Processing Service with Task Queue for Swiss Topo Data (YEAR ITERATION BUILD)
# ==============================================================================
#
# 版本说明 (YEAR ITERATION BUILD v18.2):
# -----------------
# 1. [ADDED] 新增了 /api/cancel_task/<task_id> 接口，允许前端真正地
#    取消在后台排队的任务，防止服务器资源浪费。
#
# ==============================================================================

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
from flask import Flask, request, send_file, jsonify, abort
from flask_cors import CORS
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

# --- 全局配置与状态管理 ---
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

MAX_DOWNLOAD_WORKERS = 15
MAX_PIXELS = 65536 
COLLECTION_RESOLUTIONS = {
    'ch.swisstopo.swissimage-dop10': (0.1, 0.1) 
}

TASK_QUEUE = deque()
TASK_STATUS = {}
QUEUE_LOCK = threading.Lock()

# --- Flask 应用初始化 ---
app = Flask(__name__)
CORS(app)

# --- 核心功能函数 ---

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
    update_subtask_status(task_id, subtask_key, f"正在计算所需的 {data_type} 瓦片...")
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
                # Check if task was cancelled during URL validation
                with QUEUE_LOCK:
                    if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled':
                        app.logger.info(f"Task {task_id} cancelled during URL validation.")
                        # We can't easily stop the running futures, but we can stop processing results.
                        return None 
                update_subtask_status(task_id, subtask_key, f"正在验证瓦片URL ({i + 1}/{len(tile_names_to_check)})...")
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
    
    # Check if the task was cancelled during the (potentially long) URL search
    with QUEUE_LOCK:
        if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled' or urls is None:
            return None

    if not urls:
        update_subtask_status(task_id, subtask_key, '没有找到任何有效的瓦片。')
        return None

    target_resolution = COLLECTION_RESOLUTIONS.get('ch.swisstopo.swissimage-dop10')
    resampling_method = Resampling.cubic
    
    sources_for_merging = []
    
    for i, url in enumerate(urls):
        update_subtask_status(task_id, subtask_key, f"正在从云端读取 {data_type} ({i + 1}/{len(urls)})...")
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
            app.logger.error(f"从URL处理失败: {url}, 错误: {e}")
            continue

    if not sources_for_merging:
        update_subtask_status(task_id, subtask_key, '无法从任何有效的源裁剪数据。')
        return None

    update_subtask_status(task_id, subtask_key, f"正在合并已裁剪的 {data_type} 片段...")
    
    minx, miny, maxx, maxy = geom_lv95_shape.bounds
    out_width_px = (maxx - minx) / target_resolution[0]
    out_height_px = (maxy - miny) / target_resolution[1]

    effective_resolution = target_resolution
    if max(out_width_px, out_height_px) > MAX_PIXELS:
        scale_factor = max(out_width_px, out_height_px) / MAX_PIXELS
        effective_resolution = (target_resolution[0] * scale_factor, target_resolution[1] * scale_factor)
        update_subtask_status(task_id, subtask_key, f"区域过大，自动降采样...")

    mosaic, out_trans = merge(sources_for_merging, res=effective_resolution, resampling=resampling_method)
    
    final_meta = sources_for_merging[0].meta.copy()
    final_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "transform": out_trans})

    for src in sources_for_merging:
        src.close()

    final_mem_file = io.BytesIO()
    with rasterio.open(final_mem_file, 'w', **final_meta) as dst:
        dst.write(mosaic)
    final_mem_file.seek(0)

    update_subtask_status(task_id, subtask_key, '处理完成')
    return final_mem_file

# --- 后台工作线程 ---
def worker():
    app.logger.info(f"后台工作线程启动 (PID: {os.getpid()})")
    while True:
        task_id, geometry = None, None
        with QUEUE_LOCK:
            if TASK_QUEUE:
                task_id, geometry = TASK_QUEUE.popleft()
        if task_id:
            # Check if the task was cancelled while in queue
            with QUEUE_LOCK:
                if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled':
                    app.logger.info(f"Task {task_id} was cancelled. Discarding from worker.")
                    continue

            app.logger.info(f"开始处理任务: {task_id}")
            try:
                with QUEUE_LOCK:
                    TASK_STATUS[task_id]['status'] = 'processing'
                    TASK_STATUS[task_id]['message'] = '正在转换坐标...'
                transformer = Transformer.from_crs("EPSG:4326", "EPSG:2056", always_xy=True)
                geom_lv95_shape = shapely_transform(transformer.transform, shape(geometry))
                
                with QUEUE_LOCK:
                    TASK_STATUS[task_id]['message'] = '开始处理影像...'
                    TASK_STATUS[task_id]['progress']['image']['status'] = 'processing'
                
                img_file = process_dataset_remotely(geom_lv95_shape, task_id, "影像", "image")
                
                # After processing, check again if it was cancelled during the process
                with QUEUE_LOCK:
                    if TASK_STATUS.get(task_id, {}).get('status') == 'cancelled':
                        app.logger.info(f"Task {task_id} was cancelled during processing. Halting.")
                        continue

                if not img_file:
                    raise ValueError("影像处理失败。")

                with QUEUE_LOCK:
                    TASK_STATUS[task_id]['message'] = '正在保存结果文件...'
                
                task_results_dir = os.path.join(os.getcwd(), 'results', task_id)
                os.makedirs(task_results_dir, exist_ok=True)
                
                dom_path = os.path.join(task_results_dir, 'dom_clipped.tif')
                with open(dom_path, 'wb') as f: f.write(img_file.getbuffer())
                download_urls = {'dom': f'/api/download/{task_id}/dom_clipped.tif'}

                with QUEUE_LOCK:
                    TASK_STATUS[task_id]['status'] = 'complete'
                    TASK_STATUS[task_id]['message'] = '处理完成！可直接下载。'
                    TASK_STATUS[task_id]['download_urls'] = download_urls
                app.logger.info(f"任务 {task_id} 处理完成。")

            except Exception as e:
                with QUEUE_LOCK:
                    # Don't overwrite status if it was cancelled
                    if TASK_STATUS.get(task_id, {}).get('status') != 'cancelled':
                        app.logger.error(f"任务 {task_id} 处理失败: {e}", exc_info=True)
                        TASK_STATUS[task_id]['status'] = 'error'
                        TASK_STATUS[task_id]['message'] = f"处理失败: {str(e)}"
        else:
            time.sleep(1)

# --- API 端点 ---
@app.route('/api/process_area', methods=['POST'])
def submit_task():
    json_data = request.get_json()
    if not json_data or 'geometry' not in json_data:
        abort(400, description="无效的请求：缺少 'geometry' 字段。")
    task_id = str(uuid.uuid4())
    geometry = json_data['geometry']
    with QUEUE_LOCK:
        TASK_QUEUE.append((task_id, geometry))
        position = len(TASK_QUEUE)
        TASK_STATUS[task_id] = {
            "status": "queued", "message": f"任务已加入队列，当前位置: {position}",
            "position": position,
            "progress": {
                "image": {"status": "pending", "message": "等待处理"}
            }
        }
    app.logger.info(f"任务 {task_id} 已加入队列，位置: {position}")
    return jsonify({"task_id": task_id, "status": "queued", "position": position})

@app.route('/api/task_status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    with QUEUE_LOCK: status = TASK_STATUS.get(task_id)
    if not status: abort(404, description="未找到指定的任务ID。")
    return jsonify(status)

# [NEW] Endpoint to handle task cancellation
@app.route('/api/cancel_task/<task_id>', methods=['POST'])
def cancel_task(task_id):
    with QUEUE_LOCK:
        # Mark status as cancelled
        if task_id in TASK_STATUS:
            TASK_STATUS[task_id]['status'] = 'cancelled'
            TASK_STATUS[task_id]['message'] = '任务已被用户取消。'
            app.logger.info(f"Task {task_id} marked as cancelled by user.")
        
        # Try to remove from the queue if it's still there
        # This is a bit tricky with deque, so we create a new one
        new_queue = deque()
        task_found_in_queue = False
        for item in TASK_QUEUE:
            if item[0] == task_id:
                task_found_in_queue = True
                continue
            new_queue.append(item)
        
        # If we removed it, replace the old queue
        if task_found_in_queue:
            TASK_QUEUE.clear()
            TASK_QUEUE.extend(new_queue)
            app.logger.info(f"Task {task_id} removed from queue.")

    return jsonify({"status": "success", "message": f"Task {task_id} cancellation request received."})


@app.route('/api/download/<task_id>/<filename>', methods=['GET'])
def download_file(task_id, filename):
    if filename not in ['dom_clipped.tif']:
        abort(400, "无效的文件名。")
    filepath = os.path.join(os.getcwd(), 'results', task_id, filename)
    if not os.path.exists(filepath): abort(404, "文件未找到。")
    return send_file(filepath, mimetype='image/tiff', as_attachment=True)

# --- 应用启动与后台线程管理 ---
worker_thread_started = False
if not worker_thread_started:
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()
    worker_thread_started = True

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
