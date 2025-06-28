# Model Update: Claude Sonnet 4

## Changes Made

### Updated Default Model
- **Previous**: `anthropic/claude-3-opus-20240229`
- **New**: `anthropic/claude-sonnet-4-20250514`

### Files Modified
1. **config.py**: Updated default model to Claude Sonnet 4
2. **.env.example**: Updated example to show new model

## Benefits of Claude Sonnet 4

1. **Faster Response Times**: Sonnet 4 is optimized for speed while maintaining high quality
2. **Cost Effective**: Lower token costs compared to Opus
3. **Latest Training**: More recent training data and improvements
4. **Balanced Performance**: Excellent balance between speed, cost, and capability

## How to Use

### Option 1: Default Configuration
The system will now use Claude Sonnet 4 by default. No action required.

### Option 2: Environment Variable Override
To use a different model, set in your `.env` file:
```bash
OPENROUTER_MODEL=anthropic/claude-3-opus-20240229
```

### Option 3: Runtime Override
When creating the agent:
```python
from agent.openrouter_llm import create_openrouter_llm

llm = create_openrouter_llm(model="anthropic/claude-3-opus-20240229")
agent = CDRIntelligenceAgent(llm=llm)
```

## Available Models via OpenRouter

- `anthropic/claude-sonnet-4-20250514` (Default - Recommended)
- `anthropic/claude-3-opus-20240229` (Most capable, slower)
- `anthropic/claude-3-sonnet-20240229` (Previous Sonnet version)
- `openai/gpt-4-turbo-preview`
- `google/gemini-pro`

## Performance Comparison

| Model | Speed | Quality | Cost | Use Case |
|-------|-------|---------|------|----------|
| Claude Sonnet 4 | Fast | High | Low | Default - Best for most CDR analysis |
| Claude Opus 3 | Slow | Highest | High | Complex investigations |
| GPT-4 Turbo | Medium | High | Medium | Alternative option |

## Fallback Chain
If Claude Sonnet 4 is unavailable, the system will automatically try:
1. Claude Opus 3
2. GPT-4 Turbo
3. Google Gemini Pro
4. Claude Sonnet 3

This ensures continuous operation even if a model is temporarily unavailable.