# Synthetic browser image fixtures

These 17×13 RGB fixtures contain only deterministic generated pixels, no user
content. Created with Pillow 12.3.0 JPEG quality 80; baseline uses subsampling 0,
subsampled and progressive use subsampling 2. Each pixel at linear index i is
`((i * 29) % 256, (i * 47) % 256, (i * 13) % 256)`.

`send-file.json` is the browser/native shared wire contract for baseline.jpg.
Browser tests assert its equality against both actual protocol encoders. The
native RTVI pipeline test independently verifies original bytes, format,
dimensions, caption and run policy. These fixtures exercise no provider account.
