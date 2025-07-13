# SWISSMapGen

SWISSMapGen is a simple web service designed to download high-resolution Swiss terrain (Digital Elevation Model) and satellite imagery (Digital Orthophoto).

This project features a decoupled architecture with a frontend interface and a backend processing service. It allows users to interactively select a geographic area on a map and download the corresponding data as standard GeoTIFF files.

## Key Features

- **Frontend-Backend Separation**: A clear and maintainable project structure.
- **Interactive Map Selection**: Users can easily draw a bounding box on a web map to define their area of interest.
- **Asynchronous Task Processing**: The backend handles data processing requests asynchronously, allowing users to check the status of their tasks without waiting.
- **Standard GeoTIFF Output**: The final data is delivered in the widely-used GeoTIFF format, compatible with most GIS software.

## Technology Stack

- **Backend**: Python, Flask, Rasterio, Shapely
- **Frontend**: (To be determined) - Likely HTML/CSS/JavaScript with a mapping library like Leaflet.js.

## How to Use

### Backend

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/QLwin/SWISSMapGen.git
    cd SWISSMapGen
    ```

2.  **Create a virtual environment and activate it:**
    ```bash
    python -m venv venv
    # On Windows
    venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

3.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run the backend server:**
    ```bash
    python app.py
    ```
    The server will start on `http://0.0.0.0:8080`.

### Frontend

1.  **Start the backend server** as described in the steps above.
2.  **Open your web browser** and navigate to:
    ```
    http://127.0.0.1:8080
    ```
    The web interface will be loaded, and you can start using the application.

---

# SWISSMapGen

SWISSMapGen 是一个简易的网络服务，旨在提供高分辨率的瑞士地形（数字高程模型）和卫星影像（数字正射影像）的下载功能。

该项目采用前后端分离的架构，包含一个前端用户界面和一个后端处理服务。它允许用户在地图上交互式地选择一个地理区域，并下载相应的标准 GeoTIFF 格式数据。

## 主要功能

- **前后端分离**: 清晰且易于维护的项目结构。
- **交互式地图选区**: 用户可以轻松地在网页地图上绘制一个边界框，以定义他们感兴趣的区域。
- **异步任务处理**: 后端异步处理数据请求，用户可以随时检查任务状态而无需长时间等待。
- **标准 GeoTIFF 输出**: 最终数据以广泛使用的 GeoTIFF 格式提供，与大多数 GIS 软件兼容。

## 技术栈

- **后端**: Python, Flask, Rasterio, Shapely
- **前端**: (待定) - 计划使用 HTML/CSS/JavaScript 及 Leaflet.js 等地图库。

## 如何使用

### 后端

1.  **克隆仓库:**
    ```bash
    git clone https://github.com/QLwin/SWISSMapGen.git
    cd SWISSMapGen
    ```

2.  **创建并激活虚拟环境:**
    ```bash
    python -m venv venv
    # Windows 系统
    venv\Scripts\activate
    # macOS/Linux 系统
    source venv/bin/activate
    ```

3.  **安装所需依赖:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **运行后端服务:**
    ```bash
    python app.py
    ```
    服务将启动在 `http://0.0.0.0:8080`。

### 前端

1.  **启动后端服务** (如上述步骤所示)。
2.  **打开您的网络浏览器** 并访问:
    ```
    http://127.0.0.1:8080
    ```
    网页界面将会加载，您可以开始使用该应用。
