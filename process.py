import os
import uuid
import subprocess
from pathlib import Path
import asyncio
import numpy as np
from PIL import Image
from scipy.cluster.vq import kmeans, vq
from tinydb import TinyDB
import json
import redis.asyncio as redis


# Ensure Windows event loop supports subprocesses
if os.name == 'nt':
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass


async def _run_subprocess(cmd):
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd, stdout.decode(), stderr.decode())
        return subprocess.CompletedProcess(cmd, process.returncode, stdout.decode(), stderr.decode())
    except NotImplementedError:
        # Fallback for environments/loops that don't support asyncio subprocess (e.g., some Windows loops)
        def _run_blocking():
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
            return result
        result = await asyncio.to_thread(_run_blocking)
        return subprocess.CompletedProcess(cmd, result.returncode, result.stdout, result.stderr)


async def download_video(url, output_path, quality="360p"):
    cmd = [
        'yt-dlp',
        '-f', f'best[height<={quality}][ext=mp4]/best[height<={quality}]/best',
        '--merge-output-format', 'mp4',
        '-o', str(output_path),
        url
    ]
    await _run_subprocess(cmd)

async def get_video_info(url):
    cmd = [
        'yt-dlp',
        '-J',
        '--skip-download',
        url
    ]
    result = await _run_subprocess(cmd)
    data = json.loads(result.stdout)

    duration = int(data.get('duration', 0))
    title = data.get('title', '')[:15]
    webpage_url = data.get('webpage_url', url)

    return {
        'duration_seconds': duration,
        'title': title,
        'url': webpage_url,
    }

async def extract_frames(video_path, folder_path, interval=3, scale_width=320):
    output_pattern = folder_path / 'frame_%02d.jpg'
    vf = f"fps=1/{interval},scale={scale_width}:-1"
    cmd = [
        'ffmpeg',
        '-i', str(video_path),
        '-vf', vf,
        '-q:v', '8',  # JPEG quality (2-31; higher is worse). 8 is compact yet visually ok
        '-start_number', '0',
        str(output_pattern),
        '-y'  # Overwrite output files
    ]
    await _run_subprocess(cmd)

async def analyze_frame_colors(image_path, k=2):
    def _work():
        img = Image.open(image_path).convert('RGB')
        pixels = np.array(img)
        h, w, c = pixels.shape

        pixels_reshaped = pixels.reshape((h * w, c))
        pixels_float = pixels_reshaped.astype(float)
        
        centroids, _ = kmeans(pixels_float, k)
        labels, _ = vq(pixels_float, centroids)
        unique, counts = np.unique(labels, return_counts=True)
        total_pixels = h * w
        
        results_local = []
        for i, centroid in enumerate(centroids):
            results_local.append({
                'color_rgb': [int(c) for c in centroid],
                'percentage': round(counts[i] / total_pixels * 100) if i < len(counts) else 0.0
            })
        return results_local

    return await asyncio.to_thread(_work)

async def build_page(folder_path, video_url, details, frames):  

    def rgb_style(rgb):
        return f"background-color: rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"

    name = details[0].get('name') if details else 'Video ?!'
    url = details[0].get('url') if details else video_url

    print(f"Building page for '{name}' with {len(frames)} frames...")

    frames_list = []
    for frame in frames:
        clusters = frame.get('analysis', [])
        frame_file_name = frame.get('frame_name', "no name ?!")
        img_src = f"/videos/{folder_path.name}/{frame_file_name}"
        if len(clusters) < 2:
            continue
        c1 = clusters[0].get('color_rgb', [0, 0, 0])
        p1 = clusters[0].get('percentage', 50)
        c2 = clusters[1].get('color_rgb', [0, 0, 0])
        p2 = clusters[1].get('percentage', 50)
        frames_list.append(
            f'''
<div>
    <div style="{rgb_style(c1)}; height: {p1}%"></div>
    <div style="{rgb_style(c2)}; height: {p2}%"></div>
    <img src="{img_src}"></img>
</div>
'''
        )

    HTML = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SOY</title>
    <link rel="icon" href="/static/img/rocket.png">
    <link rel="stylesheet" href="/static/css/site.css">
    <script type="module" src="/static/js/datastar.js"></script>
</head>
<body class="gc">
    <h1 class="gt-xl gm-xl"><a href="{url}">{name}</a></h1>
    <article class="gc">
        <div class="frames">
            {"".join(frames_list)}
        </div>
    </article>
</body>
</html>
'''
    try:
        tmp_path = folder_path / 'video.html.tmp'
        final_path = folder_path / 'video.html'
        tmp_path.write_text(HTML, encoding='utf-8')
        os.replace(tmp_path, final_path)
        print(f"Wrote page: {final_path}")
    except Exception as e:
        print(f"Failed writing video.html: {e}")
        raise

async def process_video(video_url, user_id=None, quality="360p"):
    folder_id = str(uuid.uuid4())
    videos_root = Path("videos")
    videos_root.mkdir(exist_ok=True)
    folder_path = videos_root / folder_id
    folder_path.mkdir(exist_ok=True)
    
    # Redis client for pub/sub updates (async)
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    async def publish_update(message):
        if user_id:
            await redis_client.publish(f"user:{user_id}", json.dumps(message))
    
    try:
        await publish_update({"status": "fetching_metadata", "message": "Getting video info...", "progress": 5})
        meta = await get_video_info(video_url)
        duration = meta['duration_seconds']
        title = meta['title']
        canonical_url = meta['url']
        if duration <= 0 or duration > 60:
            raise ValueError("Invalid or unsupported video duration (must be between 1 and 60 seconds)")

        await publish_update({"status": "downloading", "message": f"Downloading {title} in {quality}...", "progress": 15})
        video_path = folder_path / 'video.mp4'
        await download_video(video_url, video_path, quality)
        
        if not video_path.exists() or video_path.stat().st_size == 0:
            raise ValueError("Video download did not produce a valid file") 
        
        await publish_update({"status": "extracting_frames", "message": "Extracting frames...", "progress": 35})
        await extract_frames(video_path, folder_path, interval=3)
        
        db_path = folder_path / 'frames.json'
        db = TinyDB(db_path, sort_keys=True, indent=4)
        db_frames = db.table("frames")
        db_details = db.table("db_details")

        await asyncio.to_thread(db_details.insert, {
            'name': title,
            'url': canonical_url,
            'length_seconds': duration,
        })
        
        frame_files = sorted(folder_path.glob('frame_*.jpg'))
        
        if len(frame_files) == 0:
            raise ValueError("No frames were extracted from the video") 

        await publish_update({"status": "analyzing", "message": f"Analyzing {len(frame_files)} frames...", "progress": 50})
        total_frames = len(frame_files)
        for index, frame_path in enumerate(frame_files, start=1):
            frame_name = frame_path.name
            analysis = await analyze_frame_colors(frame_path, k=2)
            entry = {
                'frame_name': frame_name,
                'analysis': analysis
            }
            await asyncio.to_thread(db_frames.insert, entry)
            # Update progress between 50 and 90 during analysis
            analyze_progress = 50 + int(40 * (index / total_frames))
            await publish_update({
                "status": "analyzing",
                "message": f"Analyzed {index}/{total_frames} frames...",
                "progress": min(analyze_progress, 90)
            })

        details = db_details.all()
        frames = db_frames.all()
        await publish_update({"status": "building_page", "message": "Generating HTML page...", "progress": 95})
        await build_page(folder_path, video_url, details, frames)

        await asyncio.to_thread(video_path.unlink)
        
        await publish_update({
            "status": "complete", 
            "message": "Processing complete!",
            "video_id": folder_id,
            "video_url": f"/v/{folder_id}",
            "progress": 100
        })

    except Exception as e:
        details = {
            "status": "error",
            "message": f"{e}",
            "progress": 100
        }
        await publish_update(details)

if __name__ == '__main__':
    video_url = "https://www.youtube.com/watch?v=yIL9wLxG01M"
    asyncio.run(process_video(video_url))