import os

env_content = """# Symbio Data Engine Configuration
# ==================================

# Database (Docker)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=symbio_data
POSTGRES_USER=symbio
POSTGRES_PASSWORD=symbio_secure_password

# Vector Store (Docker)
CHROMA_HOST=localhost
CHROMA_PORT=8000

# Local AI (LM Studio)
# Configured for your specific setup
LLM_BASE_URL=http://localhost:1234/v1
LLM_MODEL=glm-4-9b-0414
LLM_API_KEY=lm-studio

# Engine Settings
SPIDER_RATE_LIMIT=1.0
# Tesseract default path for Windows
TESSERACT_CMD=C:\\Program Files\\Tesseract-OCR\\tesseract.exe
"""

def setup():
    if os.path.exists(".env"):
        print("ℹ️  .env file already exists.")
        choice = input("Overwrite? (y/N): ")
        if choice.lower() != 'y':
            print("Skipping.")
            return

    with open(".env", "w", encoding="utf-8") as f:
        f.write(env_content)
    print("✅ .env file created successfully with Local LLM settings!")

if __name__ == "__main__":
    setup()
