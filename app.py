import asyncio
from datetime import datetime

from sanic import Sanic
from datastar_py import ServerSentEventGenerator as SSE
from datastar_py.sanic import datastar_response


app = Sanic(__name__)
app.static('/static/', './static/')
app.static('/', './index.html', name="index")


@app.get("/load")
@datastar_response
async def load(request):
    while True:
        yield SSE.patch_elements(f"<span id='time'>{datetime.now().isoformat()}</span>")
        await asyncio.sleep(1)


if __name__ == "__main__":
    app.run(debug=True, auto_reload=True, access_log=False)
