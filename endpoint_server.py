import logging

from flask import Flask, request
import os

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Increase upload limit
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024 * 1024  # 10GB limit

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.data:
        print("No file part in the request.")
        return "No file part in the request.", 400

    file = request.data['file']

    if file.filename == '':
        print("No selected file.")
        return "No selected file.", 400

    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)
    print(f"File saved to {file_path}")
    return "File uploaded successfully", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9999, debug=True)