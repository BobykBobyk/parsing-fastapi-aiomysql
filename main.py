import uvicorn

if __name__ == '__main__':
    config = uvicorn.Config('app:app', port=5000, log_level='info')
    server = uvicorn.Server(config)
    server.run()
