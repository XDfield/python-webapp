'''
web框架
'''
from aiohttp import web


async def handle(request):
    return web.Response(text='Hello World!', content_type='text/html')

app = web.Application()
app.router.add_get('/', handle)

web.run_app(app, host='127.0.0.1', port=9000)
