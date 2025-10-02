import asyncio
import os
import uuid
import json
import redis.asyncio as redis
import logging

from sanic import Sanic, html
from pathlib import Path
from datastar_py import ServerSentEventGenerator as SSE
from datastar_py.sanic import datastar_response

from process import process_video

# Ensure Windows event loop supports subprocesses
if os.name == 'nt':
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass


# APP SETUP

app = Sanic(__name__)
app.static('/static/', './static/')
app.static('/videos/', './videos/', name="videos")
app.static('/', './index.html', name="index")

logging.basicConfig(filename='perso.log', encoding='utf-8', level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Redis client for pub/sub
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# @app.before_server_start
# async def setup_ctx(app):
#     app.ctx.cwd = Path.cwd()

@app.after_server_stop
async def close_db(app):
    await redis_client.aclose()

@app.on_response
async def cookie(request, response):
    if not request.cookies.get("user_id"):
        user_id = uuid.uuid4().hex
        response.add_cookie('user_id', user_id)

# ROUTES

@app.get("/status_updates")
@datastar_response
async def status_updates(request):
    videos_root = Path("videos")
    items = []
    for vid_dir in sorted(videos_root.iterdir()):
        try:
            vid_id = vid_dir.name
            frames_json = vid_dir / 'frames.json'
            data = json.loads(frames_json.read_text())
            name = data['db_details']['1']['name']
            thumb_path = f"/videos/{vid_id}/frame_00.jpg"
            items.append(f"<a href='/v/{vid_id}' class='gc'><img src='{thumb_path}' alt='{name}'><span>{name}</span></a>")
        except Exception:
            pass
        gallery_html = f"""
        <div id='gallery'>
            {''.join(items)}
        </div>
        """
    yield SSE.patch_elements(gallery_html)

    user_id = request.cookies.get('user_id')
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"user:{user_id}")

    video_url = None
    try:
        async for message in pubsub.listen():
            if message['type'] == 'message':
                data = json.loads(message['data'])
                if data.get('status') == "complete":
                    video_url = data.get('video_url')
                    yield SSE.patch_elements("<div id='status'><progress value='100' max='100'></progress><div><strong>Done:</strong> Processing complete! Redirecting...</div></div>")
                    break
                else:
                    progress = data.get('progress')
                    progress_html = f"<progress value='{progress}' max='100'></progress>" if progress is not None else ""
                    status_html = f"""
                    <div id="status">
                        {progress_html}
                        <div><strong>{data.get('status')}:</strong> {data.get('message')}</div>
                    </div>
                    """
                    yield SSE.patch_elements(status_html)
    finally:
        await pubsub.unsubscribe(f"user:{user_id}")
        await pubsub.aclose()
        yield SSE.redirect(video_url or "/")
    

@app.post("/process")
@datastar_response
async def process(request):
    data = request.form
    video_url = data.get('url')
    quality = data.get('quality', '360p')
    user_id = request.cookies.get('user_id')

    asyncio.create_task(process_video(video_url, user_id, quality))

    return SSE.patch_elements('<div id="form">yes it chief, one moment</div>')


@app.get("/v/<id>")
async def video_page(request, id):
    base_dir = Path("videos") / id
    if not base_dir.is_dir():
        return html("folder not found")

    html_path = base_dir / "video.html"
    if not html_path.is_file():
        return html("video.html not found")

    with html_path.open("r", encoding="utf-8") as f:
        return html(f.read())


if __name__ == "__main__":
    app.run(debug=True, auto_reload=True, access_log=False)
