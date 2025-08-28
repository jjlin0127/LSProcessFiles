from flask import Flask, request, render_template, send_file
from process_files import process_uploaded_files
import io
import zipfile

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    """Renders the main upload page."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handles file uploads, processing, and returns a zip file."""
    uploaded_files = request.files.getlist('files')
    
    if not uploaded_files or all(f.filename == '' for f in uploaded_files):
        return render_template('index.html', log="No files selected. Please upload at least one file.")

    # Process the files
    output_files, log_messages = process_uploaded_files(uploaded_files)

    # If there are processed files, zip them and send for download
    if output_files:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for filename, file_stream in output_files.items():
                file_stream.seek(0)
                zf.writestr(filename, file_stream.read())
        
        zip_buffer.seek(0)
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name='processed_files.zip'
        )
    
    # If no files were processed, show the log on the page
    return render_template('index.html', log=log_messages)

if __name__ == "__main__":
    app.run(debug=True)