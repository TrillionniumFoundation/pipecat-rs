//! Browser APIs are called directly from Rust. Generated wasm-bindgen glue only marshals values.
use crate::image_entry::ImageEntry;
use crate::job_groups::JobGroups;
use crate::lifecycle::{capture_allowed, playback_allowed, Lifecycle, State};
use crate::text_entry::{TextEntry, TextPayload};
use crate::{
    client_ready, offer_endpoint, protocol, transcript, websocket_endpoint, Config, InputMode,
    Transport,
};
use js_sys::{Promise, Reflect, Uint8Array};
use serde_json::{json, Value};
use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::{Rc, Weak},
};
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::{spawn_local, JsFuture};
use web_sys::*;

type Shared = Rc<RefCell<App>>;
type Callback = Closure<dyn FnMut(Event)>;
thread_local! { static APP: RefCell<Option<Shared>> = const { RefCell::new(None) }; }

struct App {
    config: Config,
    start_url: url::Url,
    lifecycle: Lifecycle,
    session: Option<Session>,
    ui_callbacks: Vec<Callback>,
    conversation: VecDeque<(Option<String>, Element)>,
    jobs: JobGroups,
}
struct Session {
    input_mode: InputMode,
    text_entry: TextEntry,
    image_entry: ImageEntry,
    image_read: Option<ImageRead>,
    mic: bool,
    media: Option<MediaStream>,
    context: AudioContext,
    capture: Option<(MediaStreamAudioSourceNode, ScriptProcessorNode, GainNode)>,
    audio_callback: Option<Closure<dyn FnMut(AudioProcessingEvent)>>,
    ws: Option<WebSocket>,
    rtc: Option<RtcPeerConnection>,
    data: Option<RtcDataChannel>,
    audio: Option<HtmlAudioElement>,
    callbacks: Vec<Callback>,
    heartbeat: Option<(i32, Closure<dyn FnMut()>)>,
    abort: AbortController,
    playback: VecDeque<(f64, AudioBufferSourceNode)>,
    next_audio: f64,
}
struct ImageRead {
    reader: FileReader,
    _callback: Callback,
}
impl Drop for ImageRead {
    fn drop(&mut self) {
        self.reader.set_onloadend(None);
        if self.reader.ready_state() == FileReader::LOADING {
            self.reader.abort();
        }
    }
}
impl Session {
    fn cancel_image(&mut self) {
        self.image_entry.cancel();
        self.image_read.take();
    }
    fn image_transport(&self) -> Result<(Transport, u64), JsValue> {
        if let Some(data) = self
            .data
            .as_ref()
            .filter(|d| d.ready_state() == RtcDataChannelState::Open)
        {
            Ok((Transport::SmallWebRtc, u64::from(data.buffered_amount())))
        } else if let Some(ws) = self
            .ws
            .as_ref()
            .filter(|w| w.ready_state() == WebSocket::OPEN)
        {
            Ok((Transport::WebSocket, u64::from(ws.buffered_amount())))
        } else {
            Err(error("Transport is disconnected; image kept"))
        }
    }
    fn clear_playback(&mut self) {
        for (_, node) in self.playback.drain(..) {
            let _ = node.unchecked_ref::<AudioScheduledSourceNode>().stop();
            let _ = node.disconnect();
        }
        self.next_audio = self.context.current_time();
    }
    fn close(&mut self) {
        self.text_entry.close();
        self.image_entry.close();
        self.image_read.take();
        self.abort.abort();
        if let Some((timer, _)) = self.heartbeat.take() {
            window().clear_interval_with_handle(timer);
        }
        if let Some(ws) = self.ws.take() {
            ws.set_onopen(None);
            ws.set_onmessage(None);
            ws.set_onerror(None);
            ws.set_onclose(None);
            let _ = ws.close();
        }
        if let Some(data) = self.data.take() {
            data.set_onopen(None);
            data.set_onmessage(None);
            data.set_onclose(None);
            data.close();
        }
        if let Some(rtc) = self.rtc.take() {
            rtc.set_ontrack(None);
            rtc.set_onconnectionstatechange(None);
            rtc.close();
        }
        if let Some(media) = self.media.take() {
            stop_media(&media);
        }
        if let Some((source, processor, gain)) = self.capture.take() {
            processor.set_onaudioprocess(None);
            let _ = processor.disconnect();
            let _ = source.disconnect();
            let _ = gain.disconnect();
        }
        if let Some(audio) = self.audio.take() {
            let _ = audio.pause();
            audio.set_src_object(None);
            audio.remove();
        }
        self.clear_playback();
        let _ = self.context.close();
        self.audio_callback.take();
        self.callbacks.clear();
    }
    fn send_text(&mut self, text: &str) -> Result<String, JsValue> {
        let (transport, buffered) = if let Some(data) = self
            .data
            .as_ref()
            .filter(|d| d.ready_state() == RtcDataChannelState::Open)
        {
            (Transport::SmallWebRtc, u64::from(data.buffered_amount()))
        } else if let Some(ws) = self
            .ws
            .as_ref()
            .filter(|w| w.ready_state() == WebSocket::OPEN)
        {
            (Transport::WebSocket, u64::from(ws.buffered_amount()))
        } else {
            return Err(error("Transport is disconnected; draft kept"));
        };
        let request = self
            .text_entry
            .prepare(text, transport, buffered)
            .map_err(|e| error(&e))?;
        match request.payload {
            TextPayload::WebRtc(json) => self.data.as_ref().unwrap().send_with_str(&json)?,
            TextPayload::WebSocket(bytes) => {
                self.ws.as_ref().unwrap().send_with_u8_array(&bytes)?
            }
        }
        Ok(request.id)
    }
    fn send(&self, value: &Value) -> Result<(), JsValue> {
        if let Some(data) = &self.data {
            if data.ready_state() == RtcDataChannelState::Open {
                return data.send_with_str(&value.to_string());
            }
        }
        if let Some(ws) = &self.ws {
            if ws.ready_state() == WebSocket::OPEN {
                return ws.send_with_u8_array(&protocol::encode_message(value));
            }
        }
        Err(error("transport is not connected"))
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

fn begin_image(app: &Shared) -> Result<(), JsValue> {
    let input: HtmlInputElement = element("image-input").unchecked_into();
    let files = input
        .files()
        .ok_or_else(|| error("Choose one PNG or JPEG image"))?;
    if files.length() != 1 {
        return Err(error("Choose exactly one PNG or JPEG image"));
    }
    let file = files
        .get(0)
        .ok_or_else(|| error("Image file is unavailable"))?;
    let size = file.size();
    if !size.is_finite() || size <= 0. || size > u32::MAX as f64 {
        return Err(error("Invalid image size"));
    }
    let mime = file.type_();
    let caption = element("image-caption")
        .unchecked_into::<HtmlTextAreaElement>()
        .value();
    if caption.len() > crate::text_entry::MAX_TEXT_BYTES {
        return Err(error("Image caption exceeds 16 KiB UTF-8"));
    }
    let selected_file = file.clone();
    let reader = FileReader::new()?;
    let (epoch, ticket, transport) = {
        let mut state = app.borrow_mut();
        if state.lifecycle.state != State::Connected {
            return Err(error(
                "Connect and wait for bot-ready before sending an image",
            ));
        }
        let epoch = state.lifecycle.generation;
        let session = state
            .session
            .as_mut()
            .ok_or_else(|| error("Transport is disconnected"))?;
        let (transport, _) = session.image_transport()?;
        let ticket = session
            .image_entry
            .begin(size as u64, &mime, transport)
            .map_err(|e| error(&e))?;
        (epoch, ticket, transport)
    };
    let weak = Rc::downgrade(app);
    let completed = reader.clone();
    let callback = Closure::wrap(Box::new(move |_: Event| {
        let Some(app) = weak.upgrade() else {
            return;
        };
        // Fence before touching the result or UI. A replaced selection and an
        // old session must never clear or submit the current user's selection.
        let mut state = app.borrow_mut();
        if !state.lifecycle.accepts(epoch) || state.lifecycle.state != State::Connected {
            return;
        }
        let Some(session) = state
            .session
            .as_mut()
            .filter(|s| s.image_entry.is_current(ticket))
        else {
            return;
        };
        let outcome = (|| -> Result<String, JsValue> {
            if completed.error().is_some() || completed.ready_state() != FileReader::DONE {
                return Err(error("Image read failed; choose or retry explicitly"));
            }
            let selection = element("image-input")
                .unchecked_into::<HtmlInputElement>()
                .files()
                .and_then(|files| files.get(0));
            if !selection.is_some_and(|f| js_sys::Object::is(f.as_ref(), selected_file.as_ref()))
                || element("image-caption")
                    .unchecked_into::<HtmlTextAreaElement>()
                    .value()
                    != caption
            {
                return Err(error(
                    "Image selection or caption changed; retry explicitly",
                ));
            }
            let result = completed.result()?;
            if !result.is_instance_of::<js_sys::ArrayBuffer>() {
                return Err(error("Image read did not produce bytes"));
            }
            let array = Uint8Array::new(&result);
            if u64::from(array.length()) != size as u64 {
                return Err(error("Image size changed during read"));
            }
            let (current_transport, buffered) = session.image_transport()?;
            if current_transport != transport {
                return Err(error("Image transport changed during read"));
            }
            let request = session
                .image_entry
                .prepare(
                    ticket,
                    &array.to_vec(),
                    &mime,
                    &caption,
                    transport,
                    buffered,
                )
                .map_err(|e| error(&e))?;
            match request.payload {
                TextPayload::WebRtc(json) => session.data.as_ref().unwrap().send_with_str(&json)?,
                TextPayload::WebSocket(bytes) => {
                    session.ws.as_ref().unwrap().send_with_u8_array(&bytes)?
                }
            }
            Ok(request.id)
        })();
        session.cancel_image();
        match outcome {
            Ok(id) => {
                element("image-input")
                    .unchecked_into::<HtmlInputElement>()
                    .set_value("");
                element("image-status").set_text_content(Some("Queued original image bytes locally; server acceptance and playback are not confirmed."));
                element("image-caption")
                    .unchecked_into::<HtmlTextAreaElement>()
                    .set_value("");
                event("image-submitted", &id);
            }
            Err(err) => {
                element("image-status").set_text_content(Some(&js_error(err)));
            }
        }
        update(&state);
    }) as Box<dyn FnMut(Event)>);
    reader.set_onloadend(Some(callback.as_ref().unchecked_ref()));
    {
        let mut state = app.borrow_mut();
        state.session.as_mut().unwrap().image_read = Some(ImageRead {
            reader: reader.clone(),
            _callback: callback,
        });
        update(&state);
    }
    if let Err(err) = reader.read_as_array_buffer(&file) {
        let mut state = app.borrow_mut();
        state.session.as_mut().unwrap().cancel_image();
        update(&state);
        return Err(err);
    }
    element("image-status").set_text_content(Some(
        "Reading selected image. Clear / Cancel stops a pending read.",
    ));
    Ok(())
}

fn window() -> Window {
    web_sys::window().expect("browser window")
}
fn document() -> Document {
    window().document().expect("browser document")
}
fn element(id: &str) -> Element {
    document().get_element_by_id(id).expect("template element")
}
fn error(message: &str) -> JsValue {
    js_sys::Error::new(message).into()
}
fn js_error(value: JsValue) -> String {
    value
        .as_string()
        .or_else(|| Reflect::get(&value, &"message".into()).ok()?.as_string())
        .unwrap_or_else(|| "browser operation failed".into())
}
fn stop_media(media: &MediaStream) {
    for track in media.get_tracks().iter() {
        track.unchecked_into::<MediaStreamTrack>().stop();
    }
}
fn current(app: &Shared, epoch: u64) -> bool {
    let app = app.borrow();
    app.lifecycle.accepts(epoch) && app.session.is_some()
}
fn checked(app: &Shared, epoch: u64) -> Result<(), JsValue> {
    if current(app, epoch) {
        Ok(())
    } else {
        Err(error("connection cancelled"))
    }
}
fn update(app: &App) {
    let button: HtmlButtonElement = element("connect-btn").unchecked_into();
    button.set_disabled(false);
    button.set_text_content(Some(match app.lifecycle.state {
        State::Idle => "Connect",
        State::Connecting => "Cancel",
        State::Connected => "Disconnect",
    }));
    let _ = button
        .class_list()
        .toggle_with_force("disconnect", app.lifecycle.state != State::Idle);
    element("transport-state").set_text_content(Some(match app.lifecycle.state {
        State::Idle => "Disconnected",
        State::Connecting => "Connecting",
        State::Connected => "Connected",
    }));
    element("transport-select")
        .unchecked_into::<HtmlSelectElement>()
        .set_disabled(app.lifecycle.state != State::Idle);
    let mic =
        app.session.as_ref().is_some_and(|s| s.mic) && app.lifecycle.state == State::Connected;
    let button: HtmlButtonElement = element("mic-btn").unchecked_into();
    button.set_disabled(
        app.lifecycle.state != State::Connected
            || !app
                .session
                .as_ref()
                .is_some_and(|s| s.input_mode.captures_microphone()),
    );
    let _ = button.class_list().toggle_with_force("on", mic);
    element("mic-status").set_text_content(Some(if app.config.input_mode == InputMode::Text {
        "Text mode (no microphone)"
    } else if mic {
        "Mic on"
    } else {
        "Mic off"
    }));
    let text_ready = app.lifecycle.state == State::Connected
        && app
            .session
            .as_ref()
            .is_some_and(|s| s.text_entry.is_ready());
    element("message-input")
        .unchecked_into::<HtmlTextAreaElement>()
        .set_disabled(!text_ready);
    element("image-caption")
        .unchecked_into::<HtmlTextAreaElement>()
        .set_disabled(!text_ready);
    element("image-input")
        .unchecked_into::<HtmlInputElement>()
        .set_disabled(!text_ready);
    element("image-send-btn")
        .unchecked_into::<HtmlButtonElement>()
        .set_disabled(!text_ready || app.session.as_ref().is_some_and(|s| s.image_read.is_some()));
    element("image-clear-btn")
        .unchecked_into::<HtmlButtonElement>()
        .set_disabled(!text_ready);
    element("send-btn")
        .unchecked_into::<HtmlButtonElement>()
        .set_disabled(!text_ready);
}
fn event(name: &str, data: &str) {
    let row = document().create_element("div").unwrap();
    row.set_class_name("event-entry");
    let stamp = js_sys::Date::new_0()
        .to_iso_string()
        .as_string()
        .unwrap_or_default();
    for (class, text) in [
        ("timestamp", stamp.get(11..23).unwrap_or("")),
        ("event-name", name),
        ("event-data", data),
    ] {
        let span = document().create_element("span").unwrap();
        span.set_class_name(class);
        let text: String = text.chars().take(2048).collect();
        span.set_text_content(Some(&text));
        let _ = row.append_child(&span);
    }
    let log = element("events-log");
    let _ = log.append_child(&row);
    while log.child_element_count() > 300 {
        if let Some(first) = log.first_element_child() {
            first.remove();
        }
    }
    let log: HtmlElement = log.unchecked_into();
    log.set_scroll_top(log.scroll_height());
}
fn disconnect(app: &Shared, reason: &str) {
    let mut app = app.borrow_mut();
    app.lifecycle.cancel();
    app.session.take();
    app.jobs.close();
    render_jobs(&app.jobs);
    element("image-input")
        .unchecked_into::<HtmlInputElement>()
        .set_value("");
    element("image-caption")
        .unchecked_into::<HtmlTextAreaElement>()
        .set_value("");
    element("image-status").set_text_content(Some(
        "Choose an image after bot-ready; sending is explicit.",
    ));
    element("message-input")
        .unchecked_into::<HtmlTextAreaElement>()
        .set_value("");
    element("message-status").set_text_content(Some("Connect and wait for bot-ready to send text"));
    update(&app);
    event("disconnected", reason);
}
fn fail(app: &Shared, epoch: u64, err: JsValue) {
    if current(app, epoch) {
        event("error", &js_error(err));
        disconnect(app, "Connection closed after error");
    }
}
fn add_transcript(app: &mut App, item: crate::Transcript) {
    if let Some(id) = &item.segment_id {
        if let Some((_, row)) = app
            .conversation
            .iter()
            .find(|(key, _)| key.as_ref() == Some(id))
        {
            if let Some(text) = row.last_element_child() {
                text.set_text_content(Some(&item.text));
            }
            return;
        }
    }
    let log = element("conversation-log");
    if app.conversation.is_empty() {
        log.set_text_content(None);
    }
    let row = document().create_element("div").unwrap();
    row.set_class_name(&format!("conversation-message {}", item.role));
    let label = document().create_element("div").unwrap();
    label.set_class_name("role");
    label.set_text_content(Some(if item.role == "user" { "You" } else { "Bot" }));
    let text = document().create_element("div").unwrap();
    text.set_text_content(Some(&item.text));
    let _ = row.append_child(&label);
    let _ = row.append_child(&text);
    let _ = log.append_child(&row);
    app.conversation.push_back((item.segment_id, row));
    while app.conversation.len() > 500 {
        if let Some((_, row)) = app.conversation.pop_front() {
            row.remove();
        }
    }
    let log: HtmlElement = log.unchecked_into();
    log.set_scroll_top(log.scroll_height());
}
// Every remote field is a text node; job payloads never become HTML, URLs or actions.
fn job_text(parent: &Element, tag: &str, class: &str, text: &str) {
    let node = document().create_element(tag).unwrap();
    node.set_class_name(class);
    node.set_text_content(Some(text));
    let _ = parent.append_child(&node);
}
fn render_jobs(jobs: &JobGroups) {
    element("job-view-status").set_text_content(Some(if jobs.is_incomplete() {
        "Limited view: some activity was omitted or removed. This is not a complete task history."
    } else {
        "Read-only activity reported in this connection. No cancellation controls."
    }));
    let list = element("job-groups");
    list.set_text_content(None);
    if jobs.groups().is_empty() {
        job_text(&list, "p", "job-empty", "No job groups displayed.");
    }
    for group in jobs.groups() {
        let row = document().create_element("section").unwrap();
        row.set_class_name("job-group");
        job_text(
            &row,
            "h3",
            "job-label",
            group.label.as_deref().unwrap_or(&group.job_id),
        );
        job_text(&row, "div", "job-id", &format!("Job ID: {}", group.job_id));
        job_text(
            &row,
            "div",
            "job-completion",
            if group.completed {
                "Group completion reported"
            } else {
                "No group completion reported"
            },
        );
        for worker in &group.workers {
            let item = document().create_element("div").unwrap();
            item.set_class_name("job-worker");
            job_text(&item, "h4", "job-worker-name", &worker.name);
            job_text(
                &item,
                "div",
                "job-worker-status",
                &worker.status.as_ref().map_or_else(
                    || {
                        if worker.completed {
                            "Worker completion reported; result omitted".to_owned()
                        } else {
                            "No worker completion reported".to_owned()
                        }
                    },
                    |status| format!("Reported status: {status}"),
                ),
            );
            for (label, value) in [("Update", &worker.data), ("Response", &worker.response)] {
                if let Some(value) = value {
                    job_text(&item, "div", "job-value-label", label);
                    job_text(&item, "pre", "job-value", value);
                }
            }
            let _ = row.append_child(&item);
        }
        let _ = list.append_child(&row);
    }
}
fn message(app: &Shared, epoch: u64, text: &str) {
    if !current(app, epoch) {
        return;
    }
    if text.len() > protocol::MAX_FRAME_BYTES {
        event("error", "RTVI message exceeds 1 MiB");
        return;
    }
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        event("error", "Invalid transport JSON");
        return;
    };
    if value["type"] == "signalling" {
        match value["message"]["type"].as_str() {
            Some("peerLeft") => disconnect(app, "Peer left"),
            Some("renegotiate") => {
                event(
                    "error",
                    "Peer requested unsupported renegotiation; reconnect to continue",
                );
                disconnect(app, "Renegotiation required");
            }
            _ => {}
        }
        return;
    }
    if value["label"] != "rtvi-ai" {
        return;
    }
    let kind = value["type"].as_str().unwrap_or("unknown");
    event(
        kind,
        &crate::diagnostic_data(value.get("data").unwrap_or(&Value::Null)).to_string(),
    );
    let mut app = app.borrow_mut();
    if app.session.as_mut().is_some_and(|s| {
        s.image_entry.accept_ready(&value);
        s.text_entry.accept_ready(&value)
    }) {
        element("message-status").set_text_content(Some(
            "Ready. Send queues text locally; it is not a delivery receipt.",
        ));
        update(&app);
    }
    if matches!(kind, "user-started-speaking" | "bot-interrupted") {
        if let Some(s) = app.session.as_mut() {
            s.clear_playback();
        }
    }
    match app.jobs.apply(epoch, &value) {
        Ok(true) => render_jobs(&app.jobs),
        Ok(false) => {}
        Err(reason) => {
            render_jobs(&app.jobs);
            event("job-view-rejected", &reason);
        }
    }
    if let Some(item) = transcript(&value) {
        add_transcript(&mut app, item);
    }
}
fn connected(app: &Shared, epoch: u64) {
    if !current(app, epoch) {
        return;
    }
    let result = {
        let mut app = app.borrow_mut();
        if !app.lifecycle.connected(epoch) {
            return;
        }
        update(&app);
        app.session.as_ref().unwrap().send(&client_ready(epoch))
    };
    if let Err(err) = result {
        fail(app, epoch, err);
    } else {
        event("connected", "Transport open; waiting for bot-ready");
    }
}
fn handler(
    weak: Weak<RefCell<App>>,
    epoch: u64,
    mut callback: impl FnMut(Shared, Event) + 'static,
) -> Callback {
    Closure::wrap(Box::new(move |evt: Event| {
        if let Some(app) = weak.upgrade() {
            if current(&app, epoch) {
                callback(app, evt);
            }
        }
    }) as Box<dyn FnMut(Event)>)
}
async fn sleep(ms: i32) -> Result<(), JsValue> {
    let promise = Promise::new(&mut |resolve, _| {
        let callback = Closure::once_into_js(move || {
            let _ = resolve.call0(&JsValue::NULL);
        });
        let _ = window()
            .set_timeout_with_callback_and_timeout_and_arguments_0(callback.unchecked_ref(), ms);
    });
    JsFuture::from(promise).await.map(|_| ())
}
async fn fetch_json(
    url: &str,
    body: Option<&Value>,
    abort: &AbortController,
) -> Result<Value, JsValue> {
    let init = RequestInit::new();
    init.set_method(if body.is_some() { "POST" } else { "GET" });
    init.set_signal(Some(&abort.signal()));
    if let Some(body) = body {
        init.set_body(&JsValue::from_str(&body.to_string()));
    }
    let request = Request::new_with_str_and_init(url, &init)?;
    request.headers().set("Accept", "application/json")?;
    if body.is_some() {
        request.headers().set("Content-Type", "application/json")?;
    }
    let abort_timeout = abort.clone();
    let timeout = Closure::wrap(Box::new(move || abort_timeout.abort()) as Box<dyn FnMut()>);
    let timer = window().set_timeout_with_callback_and_timeout_and_arguments_0(
        timeout.as_ref().unchecked_ref(),
        15000,
    )?;
    let result = async {
        let response: Response = JsFuture::from(window().fetch_with_request(&request))
            .await?
            .dyn_into()?;
        if !response.ok() {
            return Err(error(&format!(
                "Server returned HTTP {}",
                response.status()
            )));
        }
        let text = JsFuture::from(response.text()?)
            .await?
            .as_string()
            .ok_or_else(|| error("response is not text"))?;
        if text.len() > protocol::MAX_FRAME_BYTES {
            return Err(error("HTTP JSON response exceeds 1 MiB"));
        }
        let value: Value =
            serde_json::from_str(&text).map_err(|_| error("Server returned invalid JSON"))?;
        if value
            .get("error")
            .is_some_and(|v| !v.is_null() && *v != false && *v != "")
        {
            return Err(error("Server returned an error response"));
        }
        Ok(value)
    }
    .await;
    window().clear_timeout_with_handle(timer);
    drop(timeout);
    result
}
fn begin_connect(app: &Shared) {
    let (transport, input_mode) = {
        let app = app.borrow();
        let select = element("transport-select")
            .unchecked_into::<HtmlSelectElement>()
            .value();
        (
            app.config
                .transports
                .iter()
                .find(|t| t.key() == select)
                .copied(),
            app.config.input_mode,
        )
    };
    let Some(transport) = transport else {
        event("error", "Unsupported transport");
        return;
    };
    if let Err(reason) = input_mode.validate_transport(transport) {
        event("error", &reason);
        return;
    }
    let context = match AudioContext::new() {
        Ok(v) => v,
        Err(e) => {
            event("error", &js_error(e));
            return;
        }
    };
    let _ = context.resume();
    let abort = match AbortController::new() {
        Ok(v) => v,
        Err(e) => {
            let _ = context.close();
            event("error", &js_error(e));
            return;
        }
    };
    let epoch = {
        let mut app = app.borrow_mut();
        let Some(epoch) = app.lifecycle.begin() else {
            let _ = context.close();
            return;
        };
        for (segment, _) in &mut app.conversation {
            *segment = None;
        }
        app.jobs.begin(epoch);
        render_jobs(&app.jobs);
        app.session = Some(Session {
            input_mode,
            text_entry: TextEntry::new(epoch),
            image_entry: ImageEntry::new(epoch),
            image_read: None,
            mic: input_mode.captures_microphone(),
            media: None,
            context,
            capture: None,
            audio_callback: None,
            ws: None,
            rtc: None,
            data: None,
            audio: None,
            callbacks: vec![],
            heartbeat: None,
            abort,
            playback: VecDeque::new(),
            next_audio: 0.,
        });
        update(&app);
        epoch
    };
    event("connecting", transport.label());
    let app = app.clone();
    spawn_local(async move {
        if let Err(e) = connect(&app, epoch, transport).await {
            fail(&app, epoch, e);
        }
    });
}
async fn connect(app: &Shared, epoch: u64, transport: Transport) -> Result<(), JsValue> {
    checked(app, epoch)?;
    let input_mode = app.borrow().session.as_ref().unwrap().input_mode;
    input_mode
        .validate_transport(transport)
        .map_err(|reason| error(&reason))?;
    if input_mode.captures_microphone() {
        let constraints = MediaStreamConstraints::new();
        constraints.set_audio_bool(true);
        constraints.set_video_bool(false);
        let media: MediaStream = JsFuture::from(
            window()
                .navigator()
                .media_devices()?
                .get_user_media_with_constraints(&constraints)?,
        )
        .await?
        .dyn_into()?;
        if !current(app, epoch) {
            stop_media(&media);
            return Err(error("connection cancelled"));
        }
        app.borrow_mut().session.as_mut().unwrap().media = Some(media);
    }
    checked(app, epoch)?;
    let (start, abort) = {
        let app = app.borrow();
        (
            app.start_url.clone(),
            app.session.as_ref().unwrap().abort.clone(),
        )
    };
    let response = fetch_json(start.as_str(), Some(&transport.start_body()), &abort).await?;
    checked(app, epoch)?;
    match transport {
        Transport::WebSocket => connect_websocket(app, epoch, &start, &response)?,
        Transport::SmallWebRtc => connect_webrtc(app, epoch, &start, &response).await?,
        _ => return Err(error("transport adapter is not implemented")),
    }
    for _ in 0..400 {
        checked(app, epoch)?;
        if app.borrow().lifecycle.state == State::Connected {
            return Ok(());
        }
        sleep(50).await?;
    }
    Err(error("Transport connection timed out"))
}
fn connect_websocket(
    app: &Shared,
    epoch: u64,
    start: &url::Url,
    response: &Value,
) -> Result<(), JsValue> {
    let url = websocket_endpoint(start, response).map_err(|e| error(&e))?;
    let ws = WebSocket::new(url.as_str())?;
    ws.set_binary_type(BinaryType::Arraybuffer);
    let onopen = handler(Rc::downgrade(app), epoch, move |app, _| {
        connected(&app, epoch)
    });
    let onclose = handler(Rc::downgrade(app), epoch, move |app, _| {
        disconnect(&app, "WebSocket closed")
    });
    let onerror = handler(Rc::downgrade(app), epoch, move |app, _| {
        fail(&app, epoch, error("WebSocket connection failed"))
    });
    let onmessage = handler(Rc::downgrade(app), epoch, move |app, evt| {
        let evt: MessageEvent = evt.unchecked_into();
        if let Some(text) = evt.data().as_string() {
            message(&app, epoch, &text);
            return;
        }
        if !evt.data().is_instance_of::<js_sys::ArrayBuffer>() {
            event("error", "Expected binary protobuf frame");
            return;
        }
        let data = Uint8Array::new(&evt.data());
        if data.length() as usize > protocol::MAX_FRAME_BYTES {
            event("error", "WebSocket frame exceeds 1 MiB");
            return;
        }
        match protocol::decode(&data.to_vec()) {
            Ok(frame) => match frame.body.unwrap() {
                protocol::frame::Body::Message(msg) => message(&app, epoch, &msg.data),
                protocol::frame::Body::Audio(audio) => {
                    if let Err(err) = play_audio(&app, &audio) {
                        event("error", &js_error(err));
                    }
                }
                protocol::frame::Body::Interruption(_) => {
                    if let Some(session) = app.borrow_mut().session.as_mut() {
                        session.clear_playback();
                    }
                    event("interruption", "Playback cleared");
                }
                protocol::frame::Body::Text(text) => event("text-frame", &text.text),
                protocol::frame::Body::Transcription(text) => {
                    event("transcription-frame", &text.text)
                }
            },
            Err(err) => event("error", &err),
        }
    });
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    {
        let mut app = app.borrow_mut();
        let session = app.session.as_mut().unwrap();
        session.ws = Some(ws);
        session
            .callbacks
            .extend([onopen, onclose, onerror, onmessage]);
    }
    if app
        .borrow()
        .session
        .as_ref()
        .unwrap()
        .input_mode
        .captures_microphone()
    {
        start_capture(app, epoch)
    } else {
        Ok(())
    }
}
fn start_capture(app: &Shared, epoch: u64) -> Result<(), JsValue> {
    let (context, media) = {
        let app = app.borrow();
        let s = app.session.as_ref().unwrap();
        if !s.input_mode.captures_microphone() {
            return Err(error("Microphone capture is disabled in text input mode"));
        }
        (
            s.context.clone(),
            s.media
                .clone()
                .ok_or_else(|| error("Voice input has no microphone stream"))?,
        )
    };
    let source = context.create_media_stream_source(&media)?;
    #[allow(deprecated)]
    let processor=context.create_script_processor_with_buffer_size_and_number_of_input_channels_and_number_of_output_channels(2048,1,1)?;
    let gain = context.create_gain()?;
    gain.gain().set_value(0.);
    let weak = Rc::downgrade(app);
    let callback = Closure::wrap(Box::new(move |evt: AudioProcessingEvent| {
        let Some(app) = weak.upgrade() else {
            return;
        };
        if !current(&app, epoch) {
            return;
        }
        let app = app.borrow();
        if app.lifecycle.state != State::Connected {
            return;
        }
        let Some(s) = app.session.as_ref() else {
            return;
        };
        if !s.mic {
            return;
        }
        let Some(ws) = s.ws.as_ref() else {
            return;
        };
        if ws.ready_state() != WebSocket::OPEN
            || !capture_allowed(app.lifecycle.state, s.mic, ws.buffered_amount())
        {
            return;
        }
        if let Ok(samples) = evt.input_buffer().and_then(|b| b.get_channel_data(0)) {
            let bytes = protocol::encode_audio(&samples, s.context.sample_rate() as u32);
            if ws.send_with_u8_array(&bytes).is_err() {
                event("error", "Microphone send failed");
            }
        }
    }) as Box<dyn FnMut(AudioProcessingEvent)>);
    processor.set_onaudioprocess(Some(callback.as_ref().unchecked_ref()));
    source.connect_with_audio_node(&processor)?;
    processor.connect_with_audio_node(&gain)?;
    gain.connect_with_audio_node(&context.destination())?;
    let mut app = app.borrow_mut();
    let s = app.session.as_mut().unwrap();
    s.capture = Some((source, processor, gain));
    s.audio_callback = Some(callback);
    Ok(())
}
fn play_audio(app: &Shared, audio: &protocol::Audio) -> Result<(), JsValue> {
    let channels = protocol::audio_channels(audio).map_err(|e| error(&e))?;
    let mut app = app.borrow_mut();
    let Some(s) = app.session.as_mut() else {
        return Ok(());
    };
    let now = s.context.current_time();
    while s.playback.front().is_some_and(|(end, _)| *end < now) {
        if let Some((_, node)) = s.playback.pop_front() {
            let _ = node.disconnect();
        }
    }
    let duration = channels[0].len() as f64 / audio.sample_rate as f64;
    if !playback_allowed(now, s.next_audio, duration, s.playback.len()) {
        return Err(error(
            "Playback backlog exceeds three seconds; frame dropped",
        ));
    }
    let buffer = s.context.create_buffer(
        audio.num_channels,
        channels[0].len() as u32,
        audio.sample_rate as f32,
    )?;
    for (i, samples) in channels.iter().enumerate() {
        buffer.copy_to_channel(samples, i as i32)?;
    }
    let node = s.context.create_buffer_source()?;
    node.set_buffer(Some(&buffer));
    node.connect_with_audio_node(&s.context.destination())?;
    let start = s.next_audio.max(now + 0.01);
    node.start_with_when(start)?;
    s.next_audio = start + duration;
    s.playback.push_back((s.next_audio, node));
    event(
        "audio-playback",
        &format!("{} Hz, {} channels", audio.sample_rate, audio.num_channels),
    );
    Ok(())
}
async fn connect_webrtc(
    app: &Shared,
    epoch: u64,
    start: &url::Url,
    response: &Value,
) -> Result<(), JsValue> {
    let session_id = response["sessionId"]
        .as_str()
        .ok_or_else(|| error("start response lacks sessionId"))?;
    let offer_url = offer_endpoint(start, session_id).map_err(|e| error(&e))?;
    let config = RtcConfiguration::new();
    if let Some(servers) = response.pointer("/iceConfig/iceServers") {
        config.set_ice_servers(&js_sys::JSON::parse(&servers.to_string())?);
    }
    let rtc = RtcPeerConnection::new_with_configuration(&config)?;
    app.borrow_mut().session.as_mut().unwrap().rtc = Some(rtc.clone());
    let data = rtc.create_data_channel("chat");
    app.borrow_mut().session.as_mut().unwrap().data = Some(data.clone());
    let open = handler(Rc::downgrade(app), epoch, move |app, _| {
        connected(&app, epoch)
    });
    let receive = handler(Rc::downgrade(app), epoch, move |app, evt| {
        let evt: MessageEvent = evt.unchecked_into();
        if let Some(text) = evt.data().as_string() {
            message(&app, epoch, &text);
        }
    });
    let close = handler(Rc::downgrade(app), epoch, move |app, _| {
        disconnect(&app, "WebRTC data channel closed")
    });
    data.set_onopen(Some(open.as_ref().unchecked_ref()));
    data.set_onmessage(Some(receive.as_ref().unchecked_ref()));
    data.set_onclose(Some(close.as_ref().unchecked_ref()));
    let state = handler(Rc::downgrade(app), epoch, move |app, _| {
        let state = app
            .borrow()
            .session
            .as_ref()
            .and_then(|s| s.rtc.as_ref())
            .map(|p| p.connection_state());
        if let Some(state) = state {
            event("transport-state", &format!("{state:?}"));
            if matches!(
                state,
                RtcPeerConnectionState::Failed
                    | RtcPeerConnectionState::Disconnected
                    | RtcPeerConnectionState::Closed
            ) {
                disconnect(&app, "WebRTC connection ended");
            }
        }
    });
    rtc.set_onconnectionstatechange(Some(state.as_ref().unchecked_ref()));
    let track = handler(Rc::downgrade(app), epoch, move |app, evt| {
        let evt: RtcTrackEvent = evt.unchecked_into();
        if evt.track().kind() != "audio" {
            return;
        }
        let outcome = (|| -> Result<(), JsValue> {
            let stream = MediaStream::new()?;
            stream.add_track(&evt.track());
            let audio = HtmlAudioElement::new()?;
            audio.set_autoplay(true);
            audio.set_src_object(Some(&stream));
            document().body().unwrap().append_child(&audio)?;
            let play = audio.play()?;
            let mut app = app.borrow_mut();
            if let Some(s) = app.session.as_mut() {
                if let Some(old) = s.audio.replace(audio) {
                    let _ = old.pause();
                    old.set_src_object(None);
                    old.remove();
                }
            }
            spawn_local(async move {
                if JsFuture::from(play).await.is_err() {
                    event(
                        "error",
                        "Browser blocked bot audio playback; reconnect after allowing audio",
                    );
                }
            });
            Ok(())
        })();
        if let Err(err) = outcome {
            event("error", &js_error(err));
        } else {
            event("track-started", "Bot audio track");
        }
    });
    rtc.set_ontrack(Some(track.as_ref().unchecked_ref()));
    {
        let mut app = app.borrow_mut();
        let s = app.session.as_mut().unwrap();
        s.callbacks.extend([open, receive, close, state, track]);
    }
    let (input_mode, media, abort) = {
        let app = app.borrow();
        let s = app.session.as_ref().unwrap();
        (s.input_mode, s.media.clone(), s.abort.clone())
    };
    if input_mode.captures_microphone() {
        let media = media.ok_or_else(|| error("Voice WebRTC input has no microphone stream"))?;
        for track in media.get_audio_tracks().iter() {
            rtc.add_track_0(&track.unchecked_into::<MediaStreamTrack>(), &media);
        }
    } else {
        let init = RtcRtpTransceiverInit::new();
        init.set_direction(RtcRtpTransceiverDirection::Recvonly);
        rtc.add_transceiver_with_str_and_init("audio", &init);
    }
    let offer = JsFuture::from(rtc.create_offer()).await?;
    checked(app, epoch)?;
    let local: RtcSessionDescriptionInit = offer.unchecked_into();
    JsFuture::from(rtc.set_local_description(&local)).await?;
    for attempt in 0..200 {
        checked(app, epoch)?;
        if rtc.ice_gathering_state() == RtcIceGatheringState::Complete {
            break;
        }
        if attempt == 199 {
            return Err(error("ICE gathering timed out"));
        }
        sleep(50).await?;
    }
    let local = rtc
        .local_description()
        .ok_or_else(|| error("WebRTC offer is absent"))?;
    let answer = fetch_json(
        offer_url.as_str(),
        Some(&json!({"sdp":local.sdp(),"type":"offer"})),
        &abort,
    )
    .await?;
    checked(app, epoch)?;
    if answer["type"] != "answer" {
        return Err(error("Server returned an invalid SDP answer type"));
    }
    let desc = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
    desc.set_sdp(
        answer["sdp"]
            .as_str()
            .ok_or_else(|| error("Server returned no SDP answer"))?,
    );
    JsFuture::from(rtc.set_remote_description(&desc))
        .await
        .map_err(|_| error("Browser rejected the remote SDP answer"))?;
    checked(app, epoch)?;
    let weak = Rc::downgrade(app);
    let heartbeat = Closure::wrap(Box::new(move || {
        if let Some(app) = weak.upgrade() {
            if current(&app, epoch) {
                if let Some(data) = app.borrow().session.as_ref().and_then(|s| s.data.as_ref()) {
                    if data.ready_state() == RtcDataChannelState::Open {
                        let _ = data.send_with_str("ping");
                    }
                }
            }
        }
    }) as Box<dyn FnMut()>);
    let timer = window().set_interval_with_callback_and_timeout_and_arguments_0(
        heartbeat.as_ref().unchecked_ref(),
        1000,
    )?;
    app.borrow_mut().session.as_mut().unwrap().heartbeat = Some((timer, heartbeat));
    Ok(())
}

/// Initialize the static template after its configuration has loaded.
async fn initialize() -> Result<(), JsValue> {
    let value = fetch_json("./config.json", None, &AbortController::new()?).await?;
    let config: Config = serde_json::from_value(value)
        .map_err(|e| error(&format!("Invalid client configuration: {e}")))?;
    let start_url = config
        .validate(&window().location().href()?)
        .map_err(|e| error(&e))?;
    document().set_title(&config.project_name);
    element("title").set_text_content(Some(&config.project_name));
    let select = element("transport-select");
    select.set_text_content(None);
    for transport in &config.transports {
        let option = document().create_element("option")?;
        option.set_attribute("value", transport.key())?;
        option.set_text_content(Some(transport.label()));
        select.append_child(&option)?;
    }
    select
        .unchecked_ref::<HtmlElement>()
        .set_hidden(config.transports.len() == 1);
    let app = Rc::new(RefCell::new(App {
        config,
        start_url,
        lifecycle: Lifecycle::default(),
        session: None,
        ui_callbacks: vec![],
        conversation: VecDeque::new(),
        jobs: JobGroups::default(),
    }));
    let weak = Rc::downgrade(&app);
    let click = Closure::wrap(Box::new(move |_: Event| {
        if let Some(app) = weak.upgrade() {
            let idle = app.borrow().lifecycle.state == State::Idle;
            if idle {
                begin_connect(&app);
            } else {
                disconnect(&app, "Disconnected by user");
            }
        }
    }) as Box<dyn FnMut(Event)>);
    element("connect-btn")
        .add_event_listener_with_callback("click", click.as_ref().unchecked_ref())?;
    let weak = Rc::downgrade(&app);
    let mic = Closure::wrap(Box::new(move |_: Event| {
        if let Some(app) = weak.upgrade() {
            let mut app = app.borrow_mut();
            if let Some(session) = app.session.as_mut() {
                // Also reject programmatic events against the disabled control.
                if !session.input_mode.captures_microphone() {
                    return;
                }
                session.mic = !session.mic;
                if let Some(media) = &session.media {
                    for track in media.get_audio_tracks().iter() {
                        track
                            .unchecked_into::<MediaStreamTrack>()
                            .set_enabled(session.mic);
                    }
                }
                if let Some(data) = &session.data {
                    let _ = data.send_with_str(&json!({
                        "type": "signalling",
                        "message": {"type": "trackStatus", "receiver_index": 0, "enabled": session.mic}
                    }).to_string());
                }
                event("microphone", if session.mic { "enabled" } else { "muted" });
            }
            update(&app);
        }
    }) as Box<dyn FnMut(Event)>);
    element("mic-btn").add_event_listener_with_callback("click", mic.as_ref().unchecked_ref())?;
    let weak = Rc::downgrade(&app);
    let unload = Closure::wrap(Box::new(move |_: Event| {
        if let Some(app) = weak.upgrade() {
            disconnect(&app, "Page closed");
        }
    }) as Box<dyn FnMut(Event)>);
    window().add_event_listener_with_callback("pagehide", unload.as_ref().unchecked_ref())?;
    let weak = Rc::downgrade(&app);
    let submit = Closure::wrap(Box::new(move |evt: Event| {
        evt.prevent_default();
        let Some(app) = weak.upgrade() else {
            return;
        };
        let input: HtmlTextAreaElement = element("message-input").unchecked_into();
        let draft = input.value();
        let outcome = {
            let mut app = app.borrow_mut();
            if app.lifecycle.state != State::Connected {
                Err(error("Connect and wait for bot-ready before sending text"))
            } else if let Some(session) = app.session.as_mut() {
                session.send_text(&draft)
            } else {
                Err(error("Transport is disconnected"))
            }
        };
        match outcome {
            Ok(id) => {
                // No asynchronous step: a second rapid submit sees the empty draft.
                // A local socket queue success is not a confirmed conversation row.
                input.set_value("");
                element("message-status").set_text_content(Some(
                    "Queued to transport; server acceptance and playback are not confirmed.",
                ));
                event("text-submitted", &id);
            }
            Err(err) => {
                let message = js_error(err);
                element("message-status").set_text_content(Some(&message));
                event("text-not-sent", &message);
            }
        }
    }) as Box<dyn FnMut(Event)>);
    element("message-form")
        .add_event_listener_with_callback("submit", submit.as_ref().unchecked_ref())?;
    let weak = Rc::downgrade(&app);
    let image_send = Closure::wrap(Box::new(move |evt: Event| {
        evt.prevent_default();
        if let Some(app) = weak.upgrade() {
            if let Err(err) = begin_image(&app) {
                element("image-status").set_text_content(Some(&js_error(err)));
            }
        }
    }) as Box<dyn FnMut(Event)>);
    element("image-send-btn")
        .add_event_listener_with_callback("click", image_send.as_ref().unchecked_ref())?;
    let weak = Rc::downgrade(&app);
    let image_change = Closure::wrap(Box::new(move |_: Event| {
        if let Some(app) = weak.upgrade() {
            let mut app = app.borrow_mut();
            if let Some(session) = app.session.as_mut() {
                session.cancel_image();
            }
            element("image-status").set_text_content(Some(
                "Selection changed. Click Send image to submit; no file has been sent.",
            ));
            update(&app);
        }
    }) as Box<dyn FnMut(Event)>);
    element("image-input")
        .add_event_listener_with_callback("change", image_change.as_ref().unchecked_ref())?;
    element("image-caption")
        .add_event_listener_with_callback("input", image_change.as_ref().unchecked_ref())?;
    let weak = Rc::downgrade(&app);
    let image_clear = Closure::wrap(Box::new(move |_: Event| {
        if let Some(app) = weak.upgrade() {
            let mut app = app.borrow_mut();
            if let Some(session) = app.session.as_mut() {
                session.cancel_image();
            }
            element("image-input")
                .unchecked_into::<HtmlInputElement>()
                .set_value("");
            element("image-caption")
                .unchecked_into::<HtmlTextAreaElement>()
                .set_value("");
            element("image-status")
                .set_text_content(Some("Image selection and pending read cleared."));
            update(&app);
        }
    }) as Box<dyn FnMut(Event)>);
    element("image-clear-btn")
        .add_event_listener_with_callback("click", image_clear.as_ref().unchecked_ref())?;
    app.borrow_mut().ui_callbacks.extend([
        click,
        mic,
        unload,
        submit,
        image_send,
        image_change,
        image_clear,
    ]);
    update(&app.borrow());
    element("conversation-log").set_text_content(Some("Connect to start talking with your bot"));
    event("initialized", "Rust/WASM browser client ready");
    APP.with(|slot| *slot.borrow_mut() = Some(app));
    Ok(())
}

#[wasm_bindgen(start)]
pub fn main() {
    spawn_local(async {
        if let Err(err) = initialize().await {
            event("error", &js_error(err));
            element("transport-state").set_text_content(Some("Configuration error"));
        }
    });
}
