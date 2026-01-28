Do not mention Meta AI (Meta, Llama) in public docs or user-facing help by default.

Rationale:
- MetaAiTranslator is experimental/beta and should remain undocumented until GA.

Guidance for future changes:
- Keep code, tests, and CLI behavior supporting the `meta` engine, but omit it from README/help.
- If user explicitly asks about Meta, you may discuss it, but do not proactively document.
- Allowed public engines: `gemini`, `gpt`, `grok`.
- Environment variable docs should include only: `GEMINI_API_KEY`, `OPENAI_API_KEY`, `GROK_API_KEY`.
- Samples should not include llama/meta models.
- use grok-4-latest as default model for grok engine

If you need to change this policy, the maintainer will update this file.