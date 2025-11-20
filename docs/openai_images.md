# Контракт генерации изображений (OpenAI Images)

Документ фиксирует единый способ вызова OpenAI Images в проекте. Он не относится к текстовым вызовам через `LLMClient` и не использует Pydantic‑схемы.
Контракт используется для создания изображений на шаге 10.

## Как вызывать
```python
from openai import OpenAI
import base64

client = OpenAI(api_key=OPENAI_API_KEY)

result = client.images.generate(
    model="gpt-image-1",
    prompt="...",
    size="1024x1536",
    quality="medium",
    output_format="webp",
    output_compression=95,
)

image_base64 = result.data[0].b64_json
image_bytes = base64.b64decode(image_base64)

with open("sprite.webp", "wb") as f:
    f.write(image_bytes)
```

## Правила
- Никаких `response_schema` и Pydantic‑классов — это отдельный контракт.
- Параметры фиксированы (модель, размер, качество, output_format, output_compression), если бизнес‑логику нужно поменять — сначала обновляем эту страницу.
- Все возможные ошибки оборачиваем в `RetryableStepError`, чтобы раннер мог повторить job.

## Где используется
- Шаг `daily_affirmations` для сохранения превью в `./export/daily_previews`.
