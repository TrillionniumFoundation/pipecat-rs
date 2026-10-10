"""Real Chromium + loopback transport fixtures, with synthetic microphone/audio only.

Test-only JavaScript supplies a remote WebRTC peer and API instrumentation; shipped
application logic remains Rust. Requires aiohttp and playwright, uses system Chromium.
"""
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

def rtvi(kind, data):
    value = json.dumps({'label': 'rtvi-ai', 'type': kind, 'data': data}).encode()
    return blob(4, blob(1, value))

def audio_frame():
    # One second allows interruption to exercise an actually scheduled source.
    pcm = b''.join(struct.pack('<h', int(math.sin(i * 2 * math.pi * 440 / 16000) * 1000)) for i in range(16000))
    return blob(2, blob(3, pcm) + varint(4 * 8) + varint(16000) + varint(5 * 8) + varint(1))

async def main():
    state = {'audio': 0, 'ready': 0, 'ws': [], 'mode': 'normal', 'offers': [], 'starts': []}
    peer_page = None
    async def config(request):
        return web.json_response({'project_name': 'Rust Pipecat acceptance', 'start_endpoint': '/api/start', 'transports': ['websocket', 'smallwebrtc'] if state['mode'] != 'unsupported' else ['daily']})
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
                await ws.send_bytes(rtvi('bot-ready', {'version': '2.2.0'}))
                await ws.send_bytes(rtvi('user-transcription', {'text': 'partial-should-not-appear', 'final': False}))
                await ws.send_bytes(rtvi('user-transcription', {'text': 'Hello from user', 'final': True}))
                await ws.send_bytes(rtvi('bot-output', {'text': 'Initial output', 'segment_id': 42, 'spoken_status': 'new'}))
                await ws.send_bytes(rtvi('bot-output', {'text': '<img src=x onerror=alert(1)> Final output', 'segment_id': 42, 'spoken_status': 'completed'}))
                await ws.send_bytes(audio_frame())
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
                channel.send(JSON.stringify({label:'rtvi-ai',type:'bot-ready',data:{version:'2.2.0'}}));
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
            browser = await pw.chromium.launch(executable_path='/usr/bin/chromium', headless=True, args=['--no-sandbox', '--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--autoplay-policy=no-user-gesture-required'])
            context = await browser.new_context(viewport={'width': 1200, 'height': 800})
            await context.add_init_script('''
              window.capturedStreams=[]; window.audioContexts=[]; window.audioStarted=0; window.audioStopped=0; window.openSockets=[];
              const NativeSocket=window.WebSocket; window.WebSocket=class extends NativeSocket {constructor(...args){super(...args);window.openSockets.push(this);}};
              const media = navigator.mediaDevices;
              if (media) { const original=media.getUserMedia.bind(media); media.getUserMedia=async (...args)=>{ const stream=await original(...args); window.capturedStreams.push(stream); if(window.delayMedia) await new Promise(r=>setTimeout(r,500)); return stream; }; }
              const Native=window.AudioContext;
              window.AudioContext=class extends Native { constructor(...args) { super(...args); window.audioContexts.push(this); }
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
                await page.wait_for_function("window.capturedStreams.every(s=>s.getTracks().every(t=>t.readyState==='ended')) && window.audioContexts.every(c=>c.state==='closed')")
            await initialized()
            checks.append('WASM bootstrap, JSON configuration and original-layout controls render')
            await connect()
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
            await page.click('#mic-btn')
            await peer_page.wait_for_function("window.signals.some(v=>v.type==='signalling' && v.message.type==='trackStatus' && v.message.enabled===false)")
            await page.screenshot(path=str(ROOT / 'target' / 'browser-acceptance.png'))
            await peer_page.evaluate('window.channel.close()')
            await cleanup()
            await peer_page.evaluate('window.peers.forEach(p=>p.close()); window.peerContexts.forEach(c=>c.close())')
            checks.append('Real paired RTCPeerConnection: SDP offer/answer, data-channel RTVI, incoming synthetic audio track, keepalive, track-status mute and peer close')
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
