# OpenAI inline image files

The Chat Completions adapter converts native `ContentPart::File` values whose
MIME type starts with the case-sensitive prefix `image/` into an `image_url`
block with `data:{mime_type};base64,{data}`. Image filenames are not sent. This
matches `_inline_file_item` in the pinned [Python adapter](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/adapters/services/open_ai_adapter.py#L305-L313).
MIME spelling and payload bytes are preserved, without sniffing or decoding.

This is a wire-only conversion: stored native File values, detailed core
contexts, identity transforms, existing image variants, and other providers
are unchanged. PDF and other non-image File inputs retain their existing Rust
`file` / `file_data` shape and optional filename. In particular, this bounded
change does not adopt Python's rejection of other non-image MIME types and
does not broaden the PDF-only encoded user-upload ingress.

`openai_image_file` tests exact bodies, MIME boundaries, filenames, ordering,
immutability, and provider isolation. `http_simulated` also captures the exact
outgoing body from a normal `LlmContextDetailed` pipeline using only loopback
HTTP and synthetic responses. Neither suite contacts a live provider.

Diagnostics redact inline `data:image/` URLs in `image_url.url`, including
File and existing image variants, while retaining public image URLs. The
redaction acts on a newly constructed logging body, never on the runtime body
or stored context. This preserves File payload confidentiality after its wire
representation changes from `file_data` to `image_url`.
