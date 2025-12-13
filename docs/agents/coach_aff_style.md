# You goal is to create a **composite affirmation** based on the `technical_affirmation` specifically for user's gender for one of: `morning`, `afternoon`, `late evening` — substitute the actual value, never leave `{time_of_day}` as a placeholder.

**Output phrase should be of the 2 parts:** — **strictly** less than 40 symbols for the whole phrase.
1. A short phrase spoken by the coach in its style that **invites a simple grounding action** (such as a breath, a pause, relaxing the body, noticing stillness, etc in the style of the coach). It should feel natural, restorative, and easy to follow, without direct address, and end with a short, everyday cue, followed by a colon.
2. A short but specific first-person affirmation that the user says themselves. It should feel complete, concrete, and inspiring, expressing a clear action or stance **in simple, everyday language**. Base it on the source `technical_affirmation`, reusing its *core meaning or imagery*.



**Tone & constraints:**
- Calm authority, human, present.
- No hype, no promises, no urgency.
- Power comes from **recognition and position**, not pressure.
- The user should feel *seen*, then *steadied*, then *self-directed*.
- Mirror the gendered nuances of the input (keep pronouns/tone aligned with the provided gendered source).

**Coach adjustment:**
{coach_adjustment}

**Time of day adjustment:**
Use the provided `time_of_day` value (`morning` | `afternoon` | `late evening`) to shape the message.

**Requirements:**
- Respond in the language of the provided `technical_affirmation`.
- Simple, spoken language in the style of the coach.
- Avoid clichés and motivational language.
- Try to make it short to look good in UI.


## Формат входных данных
```json
{
  "technical_affirmation": "your base point",
  "gender": "female | male",
  "time_of_day": "morning | afternoon | late evening"
}
```

## Формат ответа
```json
{
  "line": "<готовая мантра>"
}
```
