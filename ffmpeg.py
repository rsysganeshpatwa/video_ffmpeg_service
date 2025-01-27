import os
import subprocess
import threading
import time
from datetime import datetime
import psutil
import signal
from flask import Flask, request, jsonify
import boto3

app = Flask(__name__)

# Globals for process management
ffmpeg_process = None
monitor_thread = None
stop_monitoring = False

BUCKET_NAME = 'pocrsibucket'
HLS_FOLDER = 'hls-test'

s3_client = boto3.client('s3')

# Utility Functions
def clear_s3_folder():
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=HLS_FOLDER)
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': objects_to_delete})
            print(f"Cleared S3 folder {HLS_FOLDER}.")
        else:
            print(f"S3 folder {HLS_FOLDER} is already empty.")
    except Exception as e:
        print(f"Error clearing S3 folder {HLS_FOLDER}: {e}")

def clear_output_folder(TEMP_DIR):
    try:
        for file in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, file)
            if os.path.isfile(file_path):
                os.unlink(file_path)
        print(f"Cleared local folder {TEMP_DIR}.")
    except Exception as e:
        print(f"Error clearing local folder {TEMP_DIR}: {e}")



def run_ffmpeg(event_file, date, output_videos):
    global ffmpeg_process
    clear_output_folder(output_videos)
    clear_s3_folder()

    ffmpeg_command = [
        '/usr/bin/ffmpeg', '-protocol_whitelist', 'file,crypto,data,https,tls,tcp', '-re', '-f', 'concat', '-safe', '0', '-i', event_file,
        '-filter_complex', '[0:v]split=1[v1]; [v1]scale=w=854:h=480[v1out]',
        '-map', '[v1out]', '-c:v:0', 'libx264', '-b:v:0', '5000k', '-maxrate:v:0', '5350k',
        '-bufsize:v:0', '3500k', '-map', 'a:0', '-c:a', 'aac', '-b:a:0', '192k', '-ac', '2',
        '-f', 'hls', '-hls_time', '6', 
        '-hls_list_size', '20',
        '-hls_flags', 'delete_segments',
        '-hls_delete_threshold', '20',
        '-hls_segment_type', 'mpegts', '-hls_segment_filename', f'{output_videos}/{date}_segment_%03d.ts',
        '-master_pl_name', 'master.m3u8', '-var_stream_map', 'v:0,a:0', f'{output_videos}/{date}_playlist.m3u8'
    ]

    try:
        print(f"Starting FFmpeg for {date}...")
        ffmpeg_process = subprocess.Popen(ffmpeg_command)
        print(f"FFmpeg process started for {date}.")
        ffmpeg_process.wait()  # Block until process ends
    except Exception as e:
        print(f"Error in FFmpeg process: {e}")

def stop_ffmpeg():
    global ffmpeg_process

    if ffmpeg_process:
        try:
            if ffmpeg_process.poll() is None:  # Check if the process is running
                print("Stopping global FFmpeg process...")
                ffmpeg_process.terminate()
                ffmpeg_process.wait(timeout=5)
                print("Global FFmpeg process stopped successfully.")
                ffmpeg_process = None
                return
            else:
                print("Global FFmpeg process already stopped.")
        except subprocess.TimeoutExpired:
            print("Global FFmpeg process did not terminate in time. Forcibly killing it...")
            os.kill(ffmpeg_process.pid, signal.SIGKILL)
            ffmpeg_process = None
        except Exception as e:
            print(f"Error stopping global FFmpeg process: {e}")
            ffmpeg_process = None

    # Stop system-wide FFmpeg processes
    print("No global FFmpeg process found or already stopped. Searching system-wide...")
    current_pid = os.getpid()  # Get the current process ID
    try:
        for process in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
            if (
                process.info['name'] == 'ffmpeg'
                or any("ffmpeg" in arg for arg in (process.info['cmdline'] or []))
            ):
                if process.info['pid'] == current_pid:
                    print(f"Skipping FFmpeg process related to this Python app (PID: {current_pid}).")
                    continue
                print(f"Found FFmpeg process (PID: {process.info['pid']}), terminating...")
                os.kill(process.info['pid'], signal.SIGTERM)
                psutil.Process(process.info['pid']).wait(timeout=5)
                print(f"FFmpeg process (PID: {process.info['pid']}) terminated successfully.")
    except psutil.NoSuchProcess:
        print("FFmpeg process already terminated.")
    except psutil.TimeoutExpired:
        print("FFmpeg process did not terminate in time. Forcibly killing it...")
        os.kill(process.info['pid'], signal.SIGKILL)
    except Exception as e:
        print(f"Error stopping FFmpeg process: {e}")



def monitor_ffmpeg(event_file, date, output_videos):
    global stop_monitoring

    while not stop_monitoring:
        if ffmpeg_process is None or ffmpeg_process.poll() is not None:
            print("FFmpeg process not running. Restarting...")
            run_ffmpeg(event_file, date, output_videos)
        time.sleep(10)  # Check every 10 seconds


@app.route('/ffmpeg/start', methods=['POST'])
def start_ffmpeg_route():
    global monitor_thread, stop_monitoring

    data = request.get_json()
    date = data.get('date')
    event_file = data.get('event_file')
    output_videos = data.get('output_video_dir')
    stop_ffmpeg()  # Stop any existing process

    if not date or not event_file:
        return jsonify({"error": "Missing 'date' or 'event_file' in request."}), 400

    stop_monitoring = False
    monitor_thread = threading.Thread(target=monitor_ffmpeg, args=(event_file, date,output_videos), daemon=True)
    monitor_thread.start()
    return jsonify({"message": "FFmpeg process started with monitoring."})


@app.route('/ffmpeg/stop', methods=['POST'])
def stop_ffmpeg_route():
    global stop_monitoring

    stop_monitoring = True
    stop_ffmpeg()
    return jsonify({"message": "FFmpeg process and monitoring stopped."})


if __name__ == "__main__":
    app.run(port=5001)
