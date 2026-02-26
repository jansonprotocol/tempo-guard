from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

def create_app() -> FastAPI:
    app = FastAPI(
        title="ATHENA: Tempo Guard",
        description="Tempo-aware predictive engine powered by ATHENA V5-L.",
        version="1.0.0",
    )

    # Allow frontend access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health_check():
        return {"status": "ok"}

    return app

app = create_app()
