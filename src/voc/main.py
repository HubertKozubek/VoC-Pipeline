from fastapi import FastAPI

app = FastAPI(title="Voice of Customer API", description="API for Steam Review Analysis")

@app.get("/")
async def root():
    return {"message": "Voice of Customer Pipeline is running"}

@app.get("/health")
async def health_check():
    return {"status": "ok"}
