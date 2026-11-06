"""Chromium IME engine, gesture audio and explicit WS recovery qualification.

Uses the actual Rust/WASM client and a loopback protobuf peer. CDP composition
is not an OS Pinyin keyboard. Audio is synthetic and browser output is muted;
native scheduling after a Connect click is not acoustic playback or hearing.
"""
import argparse
import asyncio
import hashlib
import json
import time
import traceback
from pathlib import Path

from aiohttp import web
from playwright.async_api import async_playwright
from browser_acceptance import audio_frame, decode_message, rtvi

ROOT = Path(__file__).resolve().parents[1]
DEADLINE = 10
INSTRUMENT = r"""
window.inputProbe={mediaRequests:0,contexts:[],sockets:[],starts:[],ended:0,
  compositions:[],inputs:[],clicks:[],intervals:new Map()};
const p=window.inputProbe;
if(navigator.mediaDevices) navigator.mediaDevices.getUserMedia=async()=>{
  p.mediaRequests++; throw new Error('text-mode qualification forbids capture');
};
const AC=window.AudioContext;
window.AudioContext=class extends AC {
  constructor(...args){super(...args);p.contexts.push(this);}
  createBufferSource(){
    const node=super.createBufferSource(), start=node.start.bind(node);
    node.start=(...args)=>{const result=start(...args);
      p.starts.push({state:this.state,when:args[0],time:this.currentTime,
        rate:node.buffer.sampleRate,length:node.buffer.length});return result;};
    node.addEventListener('ended',()=>p.ended++);return node;
  }
};
const WS=window.WebSocket;
window.WebSocket=class extends WS {constructor(...args){super(...args);p.sockets.push(this);}};
const interval=window.setInterval.bind(window), clear=window.clearInterval.bind(window);
window.setInterval=(fn,ms,...args)=>{const id=interval(fn,ms,...args);p.intervals.set(id,ms);return id;};
window.clearInterval=(id)=>{p.intervals.delete(id);return clear(id);};
for(const type of ['compositionstart','compositionupdate','compositionend'])
  document.addEventListener(type,e=>{if(e.target.id==='message-input')
    p.compositions.push({type:e.type,data:e.data,trusted:e.isTrusted});},true);
document.addEventListener('input',e=>{if(e.target.id==='message-input')
  p.inputs.push({type:e.inputType,data:e.data,composing:e.isComposing,trusted:e.isTrusted});},true);
document.addEventListener('click',e=>{if(e.target.id==='connect-btn')
  p.clicks.push({trusted:e.isTrusted,active:navigator.userActivation.isActive});},true);
"""
SNAPSHOT = """() => {const p=window.inputProbe;return {
  mediaRequests:p.mediaRequests,contexts:p.contexts.map(c=>({state:c.state,time:c.currentTime})),
  socketStates:p.sockets.map(s=>s.readyState),starts:p.starts,ended:p.ended,
  compositions:p.compositions,inputs:p.inputs,clicks:p.clicks,intervals:p.intervals.size};}"""


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def main(args):
    args.output.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    source = Path(__file__).resolve()
    helper = source.with_name('browser_acceptance.py')
    source_files = [source, helper] + sorted((ROOT / 'src').glob('*.rs'))
    source_hashes = {str(p.relative_to(ROOT)): sha(p) for p in source_files}
    checks, errors, cleanup_errors, snapshots = [], [], [], []
    state = {'starts': [], 'ready': [], 'texts': [], 'closed': [], 'server_errors': []}
    sockets, transports = [], []
    runner = browser = pw = page = None
    runner_started = False
    failure = version = None
    assets, browser_args = {}, []
    launch_args = ['--mute-audio', '--enable-automation']
    port = None

    async def config(request):
        return web.json_response({'project_name': 'Rust input qualification',
            'start_endpoint': '/api/start', 'transports': ['websocket'], 'input_mode': 'text'})

    async def start(request):
        body = await request.json()
        state['starts'].append(body)
        if body.get('transport') != 'websocket':
            state['server_errors'].append('unexpected transport')
            raise web.HTTPBadRequest()
        return web.json_response({'wsUrl': f'ws://127.0.0.1:{port}/ws'})

    async def websocket(request):
        ws = web.WebSocketResponse(max_msg_size=128 * 1024)
        await ws.prepare(request)
        index = len(sockets)
        sockets.append(ws)
        transports.append(request.transport)
        try:
            async for incoming in ws:
                assert incoming.type == web.WSMsgType.BINARY, incoming.type
                value = decode_message(incoming.data)
                assert value['label'] == 'rtvi-ai'
                if value['type'] == 'client-ready':
                    assert value['data']['version'] == '2.2.0'
                    state['ready'].append({'connection': index, 'id': value['id']})
                    await ws.send_bytes(rtvi('bot-ready', {'version': '2.2.0'}, value['id']))
                else:
                    assert value['type'] == 'send-text', value['type']
                    state['texts'].append({'connection': index, 'message': value})
        except Exception as error:
            state['server_errors'].append(f'{type(error).__name__}: {error}')
        finally:
            state['closed'].append(index)
        return ws

    async def until(predicate):
        async def poll():
            while not predicate():
                assert not state['server_errors'], state['server_errors']
                await asyncio.sleep(.02)
        await asyncio.wait_for(poll(), DEADLINE)

    async def scenario():
        nonlocal runner, runner_started, port, pw, browser, page, version, browser_args, assets
        if not args.assets.is_dir():
            raise FileNotFoundError(f'Rust/WASM assets missing: {args.assets}')
        assets = {str(p.relative_to(args.assets)): sha(p)
                  for p in sorted(args.assets.rglob('*')) if p.is_file()}
        app = web.Application()
        app.router.add_get('/config.json', config)
        app.router.add_post('/api/start', start)
        app.router.add_get('/ws', websocket)
        app.router.add_static('/', args.assets)
        runner = web.AppRunner(app)
        await runner.setup()
        runner_started = True
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(executable_path=str(args.browser), headless=True,
                                          chromium_sandbox=True, args=launch_args)
        version = browser.version
        browser_cdp = await browser.new_browser_cdp_session()
        browser_args = (await browser_cdp.send('Browser.getBrowserCommandLine'))['arguments']
        assert '--no-sandbox' not in browser_args
        assert not any(a.startswith('--autoplay-policy') for a in browser_args)
        assert not any('AutoplayIgnoreWebAudio' in a for a in browser_args)
        await browser_cdp.detach()
        context = await browser.new_context(viewport={'width': 1200, 'height': 800})
        context.set_default_timeout(DEADLINE * 1000)
        await context.add_init_script(INSTRUMENT)
        page = await context.new_page()
        page.on('pageerror', lambda error: errors.append(str(error)))
        await page.goto(f'http://127.0.0.1:{port}/index.html')
        await page.wait_for_function("!document.querySelector('#connect-btn').disabled")
        assert await page.evaluate('inputProbe.contexts.length===0 && inputProbe.mediaRequests===0')
        assert not state['starts'] and not state['texts']

        async def connect():
            await page.click('#connect-btn')
            await page.wait_for_function("document.querySelector('#transport-state').textContent==='Connected' && !document.querySelector('#send-btn').disabled")
            await page.wait_for_function("inputProbe.contexts.at(-1).state==='running'")
            assert await page.evaluate('inputProbe.clicks.at(-1).trusted && inputProbe.clicks.at(-1).active')
            assert await page.evaluate('inputProbe.mediaRequests===0')

        async def disconnected():
            await page.wait_for_function("document.querySelector('#transport-state').textContent==='Disconnected' && inputProbe.contexts.every(c=>c.state==='closed') && inputProbe.sockets.every(s=>s.readyState===WebSocket.CLOSED) && inputProbe.intervals.size===0")
            assert await page.input_value('#message-input') == ''
            assert await page.locator('#send-btn').is_disabled()
            assert await page.evaluate('inputProbe.mediaRequests===0')

        await connect()
        assert len(sockets) == 1 and len(state['ready']) == 1
        await sockets[0].send_bytes(audio_frame())
        await page.wait_for_function('inputProbe.starts.length===1 && inputProbe.ended===1')
        audio = await page.evaluate('inputProbe.starts[0]')
        assert audio['state'] == 'running' and audio['rate'] == 16000 and audio['length'] == 16000
        checks.append('Trusted Connect click resumes native AudioContext and schedules/ends synthetic PCM with normal autoplay policy and muted output')
        snapshots.append({'stage': 'gesture-audio', 'observed': await page.evaluate(SNAPSHOT)})

        await page.click('#message-input')
        cdp = await context.new_cdp_session(page)
        for candidate in ['中', '中文']:
            await cdp.send('Input.imeSetComposition', {'text': candidate,
                'selectionStart': len(candidate), 'selectionEnd': len(candidate)})
            assert await page.input_value('#message-input') == candidate
            assert not state['texts']
        await cdp.send('Input.insertText', {'text': '中文'})
        await page.wait_for_function("inputProbe.compositions.some(e=>e.type==='compositionend')")
        await cdp.send('Input.insertText', {'text': ' 🦀'})
        await page.press('#message-input', 'Enter')
        await cdp.send('Input.insertText', {'text': '第二行'})
        expected = '中文 🦀\n第二行'
        assert await page.input_value('#message-input') == expected
        observed = await page.evaluate(SNAPSHOT)
        events = observed['compositions']
        assert events[0]['type'] == 'compositionstart' and events[-1]['type'] == 'compositionend'
        # CDP Input.insertText finishes the composition with an untrusted
        # compositionend on Chrome 156, also observed in an independent native
        # textarea without application code. The protocol does not promise its
        # trust bit. Keep recording it, while requiring trusted start/update and
        # native composing input, the exact event sequence and committed text.
        assert all(e['trusted'] for e in events if e['type'] != 'compositionend')
        updates = [e['data'] for e in events if e['type'] == 'compositionupdate']
        assert updates[:2] == ['中', '中文'] and updates[-1] == '中文', updates
        assert sum(e['type'] == 'compositionstart' for e in events) == 1
        assert sum(e['type'] == 'compositionend' for e in events) == 1
        assert events[-1]['data'] == '中文'
        assert any(e['composing'] and e['trusted'] for e in observed['inputs'])
        await asyncio.sleep(.15)
        assert not state['texts']
        snapshots.append({'stage': 'ime-before-send', 'draft': expected, 'observed': observed})
        await page.click('#send-btn')
        await until(lambda: len(state['texts']) == 1)
        first = state['texts'][0]['message']
        assert first['data'] == {'content': expected, 'options': {'audio_response': True, 'run_immediately': True}}
        assert await page.input_value('#message-input') == ''
        checks.append('Chromium CDP IME emits composition start/update/end with recorded trust bits, preserves Chinese/emoji/newline, and sends one exact protobuf message only after Send')

        await page.click('#message-input')
        await cdp.send('Input.insertText', {'text': '未发送草稿 🦀'})
        assert await page.input_value('#message-input') == '未发送草稿 🦀'
        transports[0].abort()  # Actual loopback TCP loss, not a DOM disconnect event.
        await disconnected()
        await until(lambda: 0 in state['closed'])
        await asyncio.sleep(.3)
        assert len(state['starts']) == 1 and len(sockets) == 1 and len(state['texts']) == 1
        snapshots.append({'stage': 'tcp-loss-cleanup', 'no_auto_retry_window_ms': 300,
                          'observed': await page.evaluate(SNAPSHOT)})
        await connect()
        assert len(state['starts']) == 2 and len(sockets) == 2 and len(state['ready']) == 2
        assert state['ready'][0]['id'] != state['ready'][1]['id']
        assert await page.input_value('#message-input') == ''
        await asyncio.sleep(.3)
        assert len(state['texts']) == 1
        await page.click('#message-input')
        await cdp.send('Input.insertText', {'text': '重连后的新消息'})
        await page.click('#send-btn')
        await until(lambda: len(state['texts']) == 2)
        fresh = state['texts'][1]
        assert fresh['connection'] == 1 and fresh['message']['data']['content'] == '重连后的新消息'
        assert fresh['message']['id'] != first['id']
        checks.append('Server TCP abort clears the draft and owned resources; user-click reconnect performs no message replay and accepts one fresh explicit send')
        await page.click('#connect-btn')
        await disconnected()
        await until(lambda: len(state['closed']) == 2)
        assert len(state['texts']) == 2 and len(state['starts']) == 2 and len(state['ready']) == 2
        await cdp.detach()
        assert not state['server_errors'] and not errors, (state['server_errors'], errors)
        snapshots.append({'stage': 'final-disconnect', 'observed': await page.evaluate(SNAPSHOT)})
        checks.append('Both connections close with all observed AudioContexts, sockets and timers released and zero microphone calls')

    try:
        await asyncio.wait_for(scenario(), timeout=90)
    except BaseException as error:
        failure = {'type': type(error).__name__, 'message': str(error), 'traceback': traceback.format_exc()}
    finally:
        if failure and page is not None:
            try:
                observed = await asyncio.wait_for(page.evaluate(SNAPSHOT), timeout=3)
                snapshots.append({'stage': 'failure', 'observed': observed})
            except BaseException as error:
                snapshots.append({'stage': 'failure', 'snapshot_error': str(error)})
        async def cleanup(name, operation):
            try:
                await asyncio.wait_for(operation, timeout=10)
            except BaseException as error:
                cleanup_errors.append(f'{name}: {type(error).__name__}: {error}')
        if browser is not None:
            await cleanup('browser', browser.close())
        if pw is not None:
            await cleanup('playwright', pw.stop())
        for index, ws in enumerate(sockets):
            await cleanup(f'fixture socket {index}', ws.close())
        if runner_started:
            await cleanup('fixture server', runner.cleanup())
        final_hashes = {str(p.relative_to(ROOT)): sha(p) if p.is_file() else None for p in source_files}
        if final_hashes != source_hashes:
            cleanup_errors.append('qualification source changed during execution')
        report = {'schema_version': 1, 'result': 'failed' if failure or cleanup_errors else 'passed',
            'checks': checks, 'failure': failure, 'cleanup_errors': cleanup_errors, 'page_errors': errors,
            'elapsed_seconds': time.monotonic() - started, 'source_sha256': source_hashes,
            'source_sha256_after': final_hashes, 'asset_sha256': assets, 'browser_version': version,
            'application_source_present': (ROOT / 'src' / 'browser.rs').is_file(),
            'browser_executable': str(args.browser), 'chromium_sandbox': True,
            'launch_args': launch_args, 'browser_command_line': browser_args,
            'observations': snapshots, 'fixture': state,
            'scope': {'ime': 'Chromium IME engine via CDP; not OS Pinyin/candidate UI',
                'audio': 'Connect gesture, native scheduling, muted synthetic PCM; not hearing or all autoplay policies',
                'network': 'Loopback TCP abort and manual reconnect; not WAN/offline/SFU recovery',
                'owner': 'Protocol fixture only; no Hepta production conversation or provider'}}
        (args.output / 'browser-input-acceptance.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if failure or cleanup_errors else 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser', type=Path, default=Path('/usr/bin/chromium'))
    parser.add_argument('--assets', type=Path, default=ROOT / 'dist')
    parser.add_argument('--output', type=Path, default=ROOT / 'target')
    raise SystemExit(asyncio.run(main(parser.parse_args())))
