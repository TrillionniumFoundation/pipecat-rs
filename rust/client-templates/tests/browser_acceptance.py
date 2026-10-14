"""Real Chromium + loopback transport fixtures, with synthetic microphone/audio only.

Test-only JavaScript supplies a remote WebRTC peer and API instrumentation; shipped
application logic remains Rust. Requires aiohttp and playwright, uses system Chromium.
"""
import base64
import asyncio
import json
import math
import struct
from pathlib import Path

from aiohttp import web
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]

def varint(value):
    out = bytearray()
    while value > 127:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)

def blob(tag, body):
    return varint(tag * 8 + 2) + varint(len(body)) + body

def rtvi(kind, data, request_id=None):
    value = json.dumps({'label': 'rtvi-ai', 'type': kind, 'data': data, 'id': request_id}).encode()
    return blob(4, blob(1, value))

def decode_message(data):
    def number(offset):
        value = 0
        for shift in range(0, 70, 7):
            byte = data[offset]; offset += 1
            value |= (byte & 127) << shift
            if byte < 128: return value, offset
        raise AssertionError('oversized fixture varint')
    assert len(data) <= 128 * 1024
    tag, pos = number(0); assert tag == 34
    length, pos = number(pos); assert pos + length == len(data)
    tag, pos = number(pos); assert tag == 10
    length, pos = number(pos); assert pos + length == len(data)
    return json.loads(data[pos:])

def audio_frame():
    # One second allows interruption to exercise an actually scheduled source.
    pcm = b''.join(struct.pack('<h', int(math.sin(i * 2 * math.pi * 440 / 16000) * 1000)) for i in range(16000))
    return blob(2, blob(3, pcm) + varint(4 * 8) + varint(16000) + varint(5 * 8) + varint(1))

async def main():
    state = {'audio': 0, 'ready': 0, 'ws': [], 'mode': 'normal', 'offers': [], 'starts': [], 'texts': [], 'images': [], 'pending_ready': None}
    peer_page = None
    async def config(request):
        transports = ['websocket', 'smallwebrtc']
        if state.get('input_mode') == 'text':
            transports = ['smallwebrtc'] if state['mode'] == 'text_rtc' else ['websocket']
        if state['mode'] == 'unsupported':
            transports = ['daily']
        value = {'project_name': 'Rust Pipecat acceptance', 'start_endpoint': '/api/start', 'transports': transports}
        if 'input_mode' in state:
            value['input_mode'] = state['input_mode']
        return web.json_response(value)
    async def start(request):
        body = await request.json()
        state['starts'].append(body)
        if state['mode'] == 'failure':
            return web.json_response({'error': 'fixture failure'}, status=503)
        if state['mode'] == 'slow':
            await asyncio.sleep(0.7)
        if body['transport'] == 'websocket':
            return web.json_response({'wsUrl': f'ws://127.0.0.1:{port}/ws?existing=kept', 'token': 'fixture?token&encoded'})
        return web.json_response({'sessionId': 'fixture-session', 'iceConfig': {'iceServers': []}})
    async def websocket(request):
        assert request.query['existing'] == 'kept'
        assert request.query['token'] == 'fixture?token&encoded'
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        state['ws'].append(ws)
        async for incoming in ws:
            if incoming.type != web.WSMsgType.BINARY:
                continue
            data = incoming.data
            if data[:1] == b'\x22' and b'client-ready' in data:
                state['ready'] += 1
                assert b'2.2.0' in data
                ready_id = decode_message(data)['id']
                if state['mode'] == 'hold_ready':
                    state['pending_ready'] = (ws, ready_id)
                    await ws.send_bytes(rtvi('bot-ready', {'version': '2.2.0'}, 'stale-session'))
                else:
                    await ws.send_bytes(rtvi('bot-ready', {'version': '2.2.0'}, ready_id))
                await ws.send_bytes(rtvi('user-transcription', {'text': 'partial-should-not-appear', 'final': False}))
                await ws.send_bytes(rtvi('user-transcription', {'text': 'Hello from user', 'final': True}))
                await ws.send_bytes(rtvi('bot-output', {'text': 'Initial output', 'segment_id': 42, 'spoken_status': 'new'}))
                await ws.send_bytes(rtvi('bot-output', {'text': '<img src=x onerror=alert(1)> Final output', 'segment_id': 42, 'spoken_status': 'completed'}))
                await ws.send_bytes(audio_frame())
            elif data[:1] == b'\x22':
                value = decode_message(data)
                assert value['type'] in ('send-text', 'send-file') and value['label'] == 'rtvi-ai'
                assert value['data']['options'] == {'audio_response': True, 'run_immediately': True}
                state['images' if value['type'] == 'send-file' else 'texts'].append(value)
            elif data[:1] == b'\x12':
                state['audio'] += 1
        return ws
    async def offer(request):
        body = await request.json()
        state['offers'].append(body)
        if state['mode'] == 'slow_offer':
            await asyncio.sleep(0.7)
        result = await peer_page.evaluate('''async (offer) => {
          const pc = new RTCPeerConnection({iceServers: []});
          window.peer = pc; (window.peers ||= []).push(pc); window.signals = []; window.pings = 0;
          const context = new AudioContext(); window.peerAudio = context; (window.peerContexts ||= []).push(context);
          const oscillator = context.createOscillator(); const output = context.createMediaStreamDestination();
          oscillator.connect(output); oscillator.start();
          pc.addTrack(output.stream.getAudioTracks()[0], output.stream);
          pc.ondatachannel = ({channel}) => {
            window.channel = channel;
            channel.onmessage = ({data}) => {
              if (data === 'ping') { window.pings++; return; }
              const value = JSON.parse(data); window.signals.push(value);
              if (value.type === 'client-ready') {
                channel.send(JSON.stringify({label:'rtvi-ai',id:value.id,type:'bot-ready',data:{version:'2.2.0'}}));
                channel.send(JSON.stringify({label:'rtvi-ai',type:'bot-output',data:{text:'WebRTC fixture transcript',segment_id:99}}));
              }
            };
          };
          await pc.setRemoteDescription(offer);
          await pc.setLocalDescription(await pc.createAnswer());
          if (pc.iceGatheringState !== 'complete') await new Promise(resolve => pc.addEventListener('icegatheringstatechange', () => { if (pc.iceGatheringState === 'complete') resolve(); }));
          return {type:pc.localDescription.type,sdp:pc.localDescription.sdp,pc_id:'fixture-peer'};
        }''', body)
        return web.json_response(result)
    app = web.Application()
    app.router.add_get('/config.json', config)
    app.router.add_post('/api/start', start)
    app.router.add_post('/api/sessions/fixture-session/api/offer', offer)
    app.router.add_get('/ws', websocket)
    app.router.add_static('/', ROOT / 'dist', show_index=True)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    errors = []
    checks = []
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(executable_path='/usr/bin/chromium', headless=True, chromium_sandbox=True, args=[ '--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--autoplay-policy=no-user-gesture-required'])
            context = await browser.new_context(viewport={'width': 1200, 'height': 800})
            await context.add_init_script('''
              window.mediaRequests=0; window.captureNodes=0; window.rejectMedia=false; window.capturedStreams=[]; window.audioContexts=[]; window.audioStarted=0; window.audioStopped=0; window.openSockets=[];
              window.openPeers=[]; const NativePeer=window.RTCPeerConnection; window.RTCPeerConnection=class extends NativePeer {constructor(...args){super(...args);window.openPeers.push(this);}};
              const NativeSocket=window.WebSocket; window.WebSocket=class extends NativeSocket {constructor(...args){super(...args);window.openSockets.push(this);}};
              const media = navigator.mediaDevices;
              if (media) { const original=media.getUserMedia.bind(media); media.getUserMedia=async (...args)=>{ window.mediaRequests++; if(window.rejectMedia) throw new Error("text mode must not request microphone"); const stream=await original(...args); window.capturedStreams.push(stream); if(window.delayMedia) await new Promise(r=>setTimeout(r,500)); return stream; }; }
              const Native=window.AudioContext;
              window.AudioContext=class extends Native { constructor(...args) { super(...args); window.audioContexts.push(this); }
                createMediaStreamSource(...args) { window.captureNodes++; return super.createMediaStreamSource(...args); }
                createBufferSource() { const node=super.createBufferSource(); const start=node.start.bind(node); const stop=node.stop.bind(node); node.start=(...args)=>{window.audioStarted++;return start(...args);}; node.stop=(...args)=>{window.audioStopped++;return stop(...args);}; return node; }
              };
            ''')
            page = await context.new_page()
            page.on('pageerror', lambda error: errors.append(str(error)))
            peer_page = await context.new_page()
            await peer_page.goto(f'http://127.0.0.1:{port}/index.html')
            async def idle():
                await page.wait_for_function("document.querySelector('#transport-state').textContent === 'Disconnected'")
            async def initialized():
                await page.goto(f'http://127.0.0.1:{port}/index.html')
                await page.wait_for_function("!document.querySelector('#connect-btn').disabled")
            async def connect():
                await page.click('#connect-btn')
                await page.wait_for_function("document.querySelector('#transport-state').textContent === 'Connected'")
            async def cleanup():
                await idle()
                assert await page.locator('#send-btn').is_disabled()
                assert await page.input_value('#message-input') == ''
                before = len(state['texts'])
                await page.evaluate("document.querySelector('#message-form').requestSubmit()")
                assert len(state['texts']) == before
                await page.wait_for_function("window.capturedStreams.every(s=>s.getTracks().every(t=>t.readyState==='ended')) && window.audioContexts.every(c=>c.state==='closed')")
            async def image_checks(transport):
                async def received():
                    if transport == 'websocket': return state['images']
                    return await peer_page.evaluate("window.signals.filter(v=>v.type==='send-file')")
                fixture = (ROOT / 'tests' / 'fixtures' / 'baseline.jpg').read_bytes()
                selection = {'name': 'image.jpg', 'mimeType': 'image/jpeg', 'buffer': fixture}
                before = len(await received())
                await page.set_input_files('#image-input', selection)
                await page.fill('#image-caption', '  图片 🦀 \" \\  ')
                await page.wait_for_timeout(50)
                assert len(await received()) == before  # selection is never an upload
                await page.evaluate("document.querySelector('#image-send-btn').click(); document.querySelector('#image-send-btn').click()")
                for _ in range(100):
                    if len(await received()) == before + 1: break
                    await asyncio.sleep(.02)
                values = await received()
                assert len(values) == before + 1
                image = values[-1]
                assert base64.b64decode(image['data']['file']['source']['bytes']) == fixture
                assert image['data']['file']['source']['width'] == 17
                assert image['data']['file']['source']['height'] == 13
                assert image['data']['content'].startswith('  图片 🦀')
                assert await page.input_value('#image-caption') == ''
                assert await page.evaluate("document.querySelector('#image-input').files.length") == 0
                before += 1
                # Encoded input is checked before FileReader starts reading.
                await page.set_input_files('#image-input', {'name':'too-large.jpg','mimeType':'image/jpeg','buffer':bytes(65537 if transport=='websocket' else 8193)})
                await page.click('#image-send-btn')
                assert 'limit' in await page.locator('#image-status').inner_text()
                assert len(await received()) == before
                await page.set_input_files('#image-input', {'name':'bad.jpg','mimeType':'image/jpeg','buffer':b'not a JPEG'})
                await page.click('#image-send-btn')
                await page.wait_for_function("!document.querySelector('#image-send-btn').disabled")
                assert len(await received()) == before
                assert await page.evaluate("document.querySelector('#image-input').files.length") == 1
                # Actual synchronous cancellation of a started native FileReader.
                await page.set_input_files('#image-input', selection)
                await page.evaluate("document.querySelector('#image-send-btn').click(); document.querySelector('#image-clear-btn').click()")
                await page.wait_for_timeout(50)
                assert len(await received()) == before
                # Instrument only read timing: selection/caption changes invalidate
                # the owner before a deliberately late native result can appear.
                await page.evaluate("window.originalImageRead=FileReader.prototype.readAsArrayBuffer; window.delayedImageReads=[]; FileReader.prototype.readAsArrayBuffer=function(blob){window.delayedImageReads.push(()=>window.originalImageRead.call(this,blob));}")
                for action in ['caption', 'selection', 'clear']:
                    await page.set_input_files('#image-input', selection)
                    await page.click('#image-send-btn')
                    if action == 'caption': await page.fill('#image-caption', 'newer caption')
                    elif action == 'selection': await page.set_input_files('#image-input', selection)
                    else: await page.click('#image-clear-btn')
                    await page.evaluate("window.delayedImageReads.splice(0).forEach(read=>read())")
                    await page.wait_for_timeout(50)
                    assert len(await received()) == before
                await page.evaluate("FileReader.prototype.readAsArrayBuffer=window.originalImageRead")
                await page.click('#image-clear-btn')
                if transport == 'websocket':
                    await page.set_input_files('#image-input', selection)
                    await page.evaluate("Object.defineProperty(window.openSockets.at(-1), 'bufferedAmount', {configurable:true,get:()=>300000})")
                    await page.click('#image-send-btn')
                    await page.wait_for_function("document.querySelector('#image-status').textContent.includes('busy')")
                    assert len(await received()) == before
                    assert await page.evaluate("document.querySelector('#image-input').files.length") == 1
                    await page.evaluate("delete window.openSockets.at(-1).bufferedAmount; window.savedImageSend=window.openSockets.at(-1).send; window.openSockets.at(-1).send=()=>{throw new Error('fixture image send failure')}")
                    await page.click('#image-send-btn')
                    await page.wait_for_function("document.querySelector('#image-status').textContent.includes('fixture image send failure')")
                    assert len(await received()) == before
                    await page.evaluate("window.openSockets.at(-1).send=window.savedImageSend")
                    await page.click('#image-send-btn')
                    for _ in range(100):
                        if len(await received()) == before + 1: break
                        await asyncio.sleep(.02)
                    assert len(await received()) == before + 1
                    before += 1
                    await page.set_input_files('#image-input', selection)
                    await page.evaluate("window.delayedImageReads=[]; FileReader.prototype.readAsArrayBuffer=function(blob){window.delayedImageReads.push(()=>window.originalImageRead.call(this,blob));}")
                    await page.click('#image-send-btn')
                    await page.click('#connect-btn')
                    await cleanup()
                    await connect()
                    await page.wait_for_function("!document.querySelector('#image-send-btn').disabled")
                    await page.evaluate("window.delayedImageReads.splice(0).forEach(read=>read()); FileReader.prototype.readAsArrayBuffer=window.originalImageRead")
                    await page.wait_for_timeout(50)
                    assert len(await received()) == before
                    assert await page.evaluate("document.querySelector('#image-input').files.length") == 0
                checks.append(f'Actual {transport} image input: explicit send, original JPEG bytes, dimensions/caption, rapid click, size/signature failures, cancellation and instrumented stale reads; WS queue/send failure keeps selection')
            await initialized()
            checks.append('WASM bootstrap, JSON configuration and original-layout controls render')
            assert await page.locator('#message-input').is_disabled()
            state['mode'] = 'hold_ready'
            await connect()
            for _ in range(100):
                if state['pending_ready']: break
                await asyncio.sleep(.02)
            assert state['pending_ready']
            assert await page.locator('#send-btn').is_disabled()
            await page.evaluate("document.querySelector('#message-input').value='early'; document.querySelector('#message-form').requestSubmit()")
            assert not state['texts']
            assert await page.locator('#image-send-btn').is_disabled()
            await page.set_input_files('#image-input', {'name':'early.jpg','mimeType':'image/jpeg','buffer':(ROOT/'tests'/'fixtures'/'baseline.jpg').read_bytes()})
            await page.evaluate("document.querySelector('#image-send-btn').dispatchEvent(new Event('click'))")
            assert not state['images']
            ws, ready_id = state['pending_ready']
            await ws.send_bytes(rtvi('bot-ready', {'version': '2.2.0'}, ready_id))
            state['mode'] = 'normal'
            await page.wait_for_function("!document.querySelector('#send-btn').disabled")
            await page.wait_for_function("document.querySelector('#conversation-log').textContent.includes('Final output')")
            assert await page.locator('.conversation-message').count() == 2
            assert await page.locator('#conversation-log img').count() == 0
            assert 'partial-should-not-appear' not in await page.locator('#conversation-log').inner_text()
            await page.wait_for_function('window.audioStarted >= 1 && window.audioContexts[0].state === "running"')
            for _ in range(100):
                if state['audio'] > 2: break
                await asyncio.sleep(.03)
            assert state['audio'] > 2 and state['ready'] == 1
            checks.append('Real WebSocket open, encoded token/query, protobuf client-ready, fake-microphone PCM capture and scheduled PCM playback')
            checks.append('RTVI v2 final transcript projection, segment upsert and text-only injection-safe rendering')
            typed = '  你好，世界！🦀\n"literal" <script> & '
            await page.fill('#message-input', typed)
            await page.press('#message-input', 'Enter')
            draft = await page.input_value('#message-input')
            assert not state['texts']  # Enter/IME text is not a submit shortcut.
            await page.evaluate("document.querySelector('#message-form').requestSubmit(); document.querySelector('#message-form').requestSubmit()")
            for _ in range(100):
                if state['texts']: break
                await asyncio.sleep(.02)
            assert len(state['texts']) == 1 and state['texts'][0]['data']['content'] == draft
            assert await page.input_value('#message-input') == ''
            assert await page.locator('.conversation-message').count() == 2
            assert await page.locator('#message-input').get_attribute('maxlength') == '16385'
            await page.fill('#message-input', 'x' * 16385)
            await page.click('#send-btn')
            assert await page.input_value('#message-input') == 'x' * 16385
            assert len(state['texts']) == 1
            await page.fill('#message-input', '中' * 6000)
            await page.click('#send-btn')
            assert await page.input_value('#message-input') == '中' * 6000
            assert len(state['texts']) == 1
            await page.fill('#message-input', 'retry explicitly')
            await page.evaluate("Object.defineProperty(window.openSockets.at(-1), 'bufferedAmount', {configurable:true,get:()=>300000})")
            await page.click('#send-btn')
            assert await page.input_value('#message-input') == 'retry explicitly'
            assert len(state['texts']) == 1
            await page.evaluate("delete window.openSockets.at(-1).bufferedAmount; window.savedSend=window.openSockets.at(-1).send; window.openSockets.at(-1).send=()=>{throw new Error('fixture send failure')}")
            await page.click('#send-btn')
            assert await page.input_value('#message-input') == 'retry explicitly'
            assert len(state['texts']) == 1
            await page.evaluate("window.openSockets.at(-1).send=window.savedSend")
            await page.click('#send-btn')
            for _ in range(100):
                if len(state['texts']) == 2: break
                await asyncio.sleep(.02)
            assert len(state['texts']) == 2
            assert state['texts'][0]['id'] != state['texts'][1]['id']
            await image_checks('websocket')
            checks.append('Actual form/WS text: exact current ready, Chinese literal payload, newline, rapid-submit single send, UTF-8 cap; instrumented bufferedAmount/send failure preserves draft; local queue success is not a conversation row')
            await page.click('#mic-btn')
            await asyncio.sleep(.15)
            muted = state['audio']
            await asyncio.sleep(.15)
            assert state['audio'] == muted
            assert await page.evaluate('window.capturedStreams.at(-1).getAudioTracks()[0].enabled') is False
            await page.click('#mic-btn')
            await asyncio.sleep(.15)
            assert state['audio'] > muted
            await state['ws'][-1].send_bytes(audio_frame())
            await page.wait_for_function('window.audioStarted >= 2')
            await state['ws'][-1].send_bytes(b'\x2a\x00')
            await page.wait_for_function('window.audioStopped > 0')
            checks.append('Microphone mute/unmute controls capture and interruption stops scheduled audio')
            await page.evaluate("Object.defineProperty(window.openSockets.at(-1), 'bufferedAmount', {configurable:true,get:()=>300000})")
            await asyncio.sleep(.15)
            paused = state['audio']
            await asyncio.sleep(.15)
            assert state['audio'] == paused
            await page.evaluate("delete window.openSockets.at(-1).bufferedAmount")
            await asyncio.sleep(.15)
            assert state['audio'] > paused
            for _ in range(5):
                await state['ws'][-1].send_bytes(audio_frame())
            await page.wait_for_function("document.querySelector('#events-log').textContent.includes('Playback backlog exceeds three seconds')")
            await state['ws'][-1].send_bytes(b'\x2a\x00')
            for i in range(310):
                await state['ws'][-1].send_bytes(rtvi('fixture-event', {'index': i}))
            await page.wait_for_function("document.querySelector('#events-log').textContent.includes('309')")
            assert await page.locator('.event-entry').count() == 300
            checks.append('Simulated WebSocket backpressure gates capture, real scheduled playback has a three-second limit, and event rows are capped at 300')
            await page.click('#connect-btn')
            await cleanup()
            await connect()
            await page.click('#connect-btn')
            await cleanup()
            checks.append('Disconnect/reconnect closes tracks, AudioContexts, WebSocket and capture graph')
            state['mode'] = 'slow'
            await page.click('#connect-btn')
            await page.wait_for_timeout(100)
            await page.click('#connect-btn')
            state['mode'] = 'normal'
            await connect()
            await page.wait_for_timeout(800)
            assert await page.locator('#transport-state').inner_text() == 'Connected'
            await page.click('#connect-btn')
            await cleanup()
            checks.append('Cancel during pending HTTP start and immediate reconnect reject stale completion')
            await page.evaluate('window.delayMedia = true')
            await page.click('#connect-btn')
            await page.wait_for_timeout(50)
            await page.click('#connect-btn')
            await page.wait_for_timeout(550)
            await cleanup()
            await page.evaluate('window.delayMedia = false')
            checks.append('Cancel during pending getUserMedia stops the late-arriving stream')
            state['mode'] = 'failure'
            await page.click('#connect-btn')
            await cleanup()
            assert 'Server returned HTTP 503' in await page.locator('#events-log').inner_text()
            state['mode'] = 'normal'
            checks.append('HTTP start failure returns to usable idle state and releases microphone')
            await page.select_option('#transport-select', 'smallwebrtc')
            state['mode'] = 'slow_offer'
            await page.click('#connect-btn')
            for _ in range(200):
                if state['offers']: break
                await asyncio.sleep(.025)
            assert state['offers']
            await page.click('#connect-btn')
            state['mode'] = 'normal'
            await connect()
            await page.wait_for_timeout(800)
            assert await page.locator('#transport-state').inner_text() == 'Connected'
            checks.append('Cancel during pending SDP offer and immediate reconnect reject the stale answer')
            await page.wait_for_function("document.querySelector('#conversation-log').textContent.includes('WebRTC fixture transcript')")
            await page.wait_for_function("document.querySelector('audio')?.srcObject?.getAudioTracks().length === 1")
            await peer_page.wait_for_function('window.pings > 0')
            assert len(state['offers']) == 2
            assert 'm=audio' in state['offers'][0]['sdp']
            await page.wait_for_function("!document.querySelector('#send-btn').disabled")
            await page.fill('#message-input', 'WebRTC 你好 🦀')
            await page.click('#send-btn')
            await peer_page.wait_for_function("window.signals.some(v=>v.type==='send-text')")
            received = await peer_page.evaluate("window.signals.filter(v=>v.type==='send-text')")
            assert len(received) == 1 and received[0]['data']['content'] == 'WebRTC 你好 🦀'
            assert received[0]['data']['options'] == {'audio_response': True, 'run_immediately': True}
            await image_checks('smallwebrtc')
            checks.append('Actual browser WebRTC form emits the bounded RTVI send-text JSON on the paired data channel')
            await page.click('#mic-btn')
            await peer_page.wait_for_function("window.signals.some(v=>v.type==='signalling' && v.message.type==='trackStatus' && v.message.enabled===false)")
            await page.screenshot(path=str(ROOT / 'target' / 'browser-acceptance.png'))
            await peer_page.evaluate('window.channel.close()')
            await cleanup()
            await peer_page.evaluate('window.peers.forEach(p=>p.close()); window.peerContexts.forEach(c=>c.close())')
            checks.append('Real paired RTCPeerConnection: SDP offer/answer, data-channel RTVI, incoming synthetic audio track, keepalive, track-status mute and peer close')
            # Explicit page configuration; this is not an in-session mode toggle.
            state['input_mode'] = 'text'
            state['mode'] = 'normal'
            await initialized()
            await page.evaluate('window.rejectMedia = true')
            assert await page.locator('#mic-btn').is_disabled()
            assert 'Text mode' in await page.locator('#mic-status').inner_text()
            audio_before = state['audio']
            text_before = len(state['texts'])
            for index in range(2):
                await connect()
                await page.wait_for_function("!document.querySelector('#send-btn').disabled")
                await page.wait_for_function('(count) => window.audioStarted >= count', arg=index + 1)
                await page.evaluate("document.querySelector('#mic-btn').dispatchEvent(new Event('click'))")
                assert await page.locator('#mic-btn').is_disabled()
                assert await page.evaluate('window.mediaRequests === 0 && window.captureNodes === 0 && window.capturedStreams.length === 0')
                assert state['audio'] == audio_before
                await page.fill('#message-input', '免麦克风 你好 🦀')
                await page.click('#send-btn')
                for _ in range(100):
                    if len(state['texts']) == text_before + index + 1: break
                    await asyncio.sleep(.02)
                assert len(state['texts']) == text_before + index + 1
                assert state['texts'][-1]['data']['content'] == '免麦克风 你好 🦀'
                await page.click('#connect-btn')
                await cleanup()
                await page.wait_for_function('window.openSockets.every(s=>s.readyState===WebSocket.CLOSED)')
            assert state['texts'][-1]['id'] != state['texts'][-2]['id']
            state['mode'] = 'failure'
            await page.click('#connect-btn')
            await cleanup()
            assert await page.evaluate('window.mediaRequests === 0 && window.captureNodes === 0')
            state['mode'] = 'slow'
            await page.click('#connect-btn')
            await page.wait_for_timeout(100)
            await page.click('#connect-btn')
            state['mode'] = 'normal'
            await connect()
            await page.wait_for_timeout(800)
            assert await page.locator('#transport-state').inner_text() == 'Connected'
            assert await page.evaluate('window.mediaRequests === 0 && window.captureNodes === 0')
            await page.click('#connect-btn')
            await cleanup()
            checks.append('Explicit WS text mode never calls throwing getUserMedia or creates capture nodes; real text/audio, inert mic control, reconnect, failure/cancel cleanup and unique session IDs')
            state['mode'] = 'text_rtc'
            await initialized()
            await page.evaluate('window.rejectMedia = true')
            for index in range(2):
                await connect()
                await page.wait_for_function("!document.querySelector('#send-btn').disabled")
                await page.wait_for_function("document.querySelector('audio')?.srcObject?.getAudioTracks().length === 1")
                await page.wait_for_function("window.openPeers.some(p=>p.connectionState==='connected' && p.getTransceivers().some(t=>t.receiver.track.kind==='audio' && t.currentDirection==='recvonly'))")
                assert await page.evaluate("window.openPeers.every(p=>p.getSenders().every(s=>s.track===null))")
                assert await page.evaluate('window.mediaRequests === 0 && window.captureNodes === 0 && window.capturedStreams.length === 0')
                await page.fill('#message-input', f'RTC 免麦克风 {index} 🦀')
                await page.click('#send-btn')
                await peer_page.wait_for_function("window.signals.some(v=>v.type==='send-text')")
                values = await peer_page.evaluate("window.signals.filter(v=>v.type==='send-text')")
                assert len(values) == 1 and values[0]['data']['content'] == f'RTC 免麦克风 {index} 🦀'
                signals_before = await peer_page.evaluate('window.signals.length')
                await page.evaluate("document.querySelector('#mic-btn').dispatchEvent(new Event('click'))")
                await page.wait_for_timeout(100)
                assert await page.locator('#mic-btn').is_disabled()
                assert await peer_page.evaluate('window.signals.length') == signals_before
                await page.wait_for_function("async()=>{const p=window.openPeers.find(p=>p.connectionState==='connected'); if(!p)return false;const stats=[...(await p.getStats()).values()];return stats.some(s=>s.type==='inbound-rtp' && s.kind==='audio' && s.packetsReceived>0);}")
                assert await page.evaluate("async()=>{const p=window.openPeers.find(p=>p.connectionState==='connected');return [...(await p.getStats()).values()].filter(s=>s.type==='outbound-rtp' && s.kind==='audio').every(s=>(s.packetsSent||0)===0);}")
                assert await peer_page.evaluate("async()=>[...(await window.peer.getStats()).values()].filter(s=>s.type==='inbound-rtp' && s.kind==='audio').every(s=>(s.packetsReceived||0)===0)")
                await page.click('#connect-btn')
                await cleanup()
                await page.wait_for_function("window.openPeers.every(p=>p.signalingState==='closed')")
                await peer_page.evaluate('window.peers.forEach(p=>p.close()); window.peerContexts.forEach(c=>c.close())')
            state['mode'] = 'slow_offer'
            offers_before = len(state['offers'])
            await page.click('#connect-btn')
            for _ in range(200):
                if len(state['offers']) > offers_before: break
                await asyncio.sleep(.025)
            assert len(state['offers']) > offers_before
            await page.click('#connect-btn')
            await cleanup()
            await page.wait_for_timeout(800)
            assert await page.evaluate("window.openPeers.every(p=>p.signalingState==='closed') && window.mediaRequests===0")
            await peer_page.evaluate('window.peers.forEach(p=>p.close()); window.peerContexts.forEach(c=>c.close())')
            checks.append('Text WebRTC: throwing permission trap untouched, no capture/local tracks, negotiated recvonly, exact RTVI text/downlink RTP, zero audio RTP uplink counters, inert mic, reconnect and pending-offer cancellation cleanup')
            del state['input_mode']
            state['mode'] = 'normal'
            await initialized()
            await connect()
            await page.wait_for_function('window.mediaRequests > 0 && window.capturedStreams.length > 0 && window.captureNodes > 0')
            await page.click('#connect-btn')
            await cleanup()
            checks.append('Disconnect/reload with omitted input_mode restores the original microphone capture path')
            state['mode'] = 'unsupported'
            await page.reload()
            await page.wait_for_function("document.querySelector('#transport-state').textContent==='Configuration error'")
            assert await page.locator('#connect-btn').is_disabled()
            checks.append('Unsupported Daily adapter fails explicitly before starting a session')
            assert not errors, errors
            await browser.close()
    finally:
        for ws in state['ws']:
            await ws.close()
        await runner.cleanup()
    output = {'result': 'passed', 'checks': checks, 'page_errors': errors, 'microphone': 'Chromium synthetic device only', 'provider': 'Loopback transport fixtures only; no live bot/provider'}
    (ROOT / 'target' / 'browser-acceptance.json').write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps(output, indent=2))

if __name__ == '__main__':
    asyncio.run(main())
