from flask import Flask, jsonify, request, send_from_directory, abort, send_file
from flask_cors import CORS
import os
import uuid
import threading
from tasks import worker, TASK_QUEUE, TASK_STATUS, QUEUE_LOCK

app = Flask(__name__)
CORS(app)

# Ensure the results directory exists
if not os.path.exists('results'):
    os.makedirs('results')

# Serve the frontend
@app.route('/')
def serve_frontend():
    return send_from_directory('static', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory('static', path)

@app.route('/api/process_area', methods=['POST'])
def submit_task():
    json_data = request.get_json()
    if not json_data or 'geometry' not in json_data:
        abort(400, description="Invalid request: missing 'geometry' field.")
    task_id = str(uuid.uuid4())
    geometry = json_data['geometry']
    with QUEUE_LOCK:
        TASK_QUEUE.append((task_id, geometry))
        position = len(TASK_QUEUE)
        TASK_STATUS[task_id] = {
            "status": "queued", "message": f"Task queued, position: {position}",
            "position": position,
            "progress": {
                "image": {"status": "pending", "message": "Waiting for processing"}
            }
        }
    print(f"Task {task_id} queued at position: {position}")
    return jsonify({"task_id": task_id, "status": "queued", "position": position})

@app.route('/api/task_status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    with QUEUE_LOCK: status = TASK_STATUS.get(task_id)
    if not status: abort(404, description="Task ID not found.")
    return jsonify(status)

@app.route('/api/cancel_task/<task_id>', methods=['POST'])
def cancel_task(task_id):
    with QUEUE_LOCK:
        if task_id in TASK_STATUS:
            TASK_STATUS[task_id]['status'] = 'cancelled'
            TASK_STATUS[task_id]['message'] = 'Task cancelled by user.'
            print(f"Task {task_id} marked as cancelled by user.")
        
        new_queue = deque()
        task_found_in_queue = False
        for item in TASK_QUEUE:
            if item[0] == task_id:
                task_found_in_queue = True
                continue
            new_queue.append(item)
        
        if task_found_in_queue:
            TASK_QUEUE.clear()
            TASK_QUEUE.extend(new_queue)
            print(f"Task {task_id} removed from queue.")

    return jsonify({"status": "success", "message": f"Task {task_id} cancellation request received."})


@app.route('/api/download/<task_id>/<filename>', methods=['GET'])
def download_file(task_id, filename):
    if filename not in ['dom_clipped.tif']:
        abort(400, "Invalid filename.")
    filepath = os.path.join(os.getcwd(), 'results', task_id, filename)
    if not os.path.exists(filepath): abort(404, "File not found.")
    return send_file(filepath, mimetype='image/tiff', as_attachment=True)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "ok"}), 200


from cheroot.wsgi import Server as CherryPyWSGIServer

if __name__ == '__main__':
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()
    print("Starting CherryPy server...")
    server = CherryPyWSGIServer(('0.0.0.0', 8080), app)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()
    print("CherryPy server stopped.")
