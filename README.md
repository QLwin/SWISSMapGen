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
