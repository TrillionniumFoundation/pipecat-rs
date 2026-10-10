//! Stateful Markdown-to-spoken-text filtering, including source whitespace rules.
use pulldown_cmark::{html, Options, Parser};
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MarkdownSettings {
    pub enable_text_filter: bool,
    pub filter_code: bool,
    pub filter_tables: bool,
    pub filter_repeated_sequences: bool,
}
impl Default for MarkdownSettings {
    fn default() -> Self {
        Self {
            enable_text_filter: true,
            filter_code: false,
            filter_tables: false,
            filter_repeated_sequences: true,
        }
    }
}
#[derive(Default)]
pub struct MarkdownTextFilter {
    settings: MarkdownSettings,
    in_code_block: bool,
    in_table: bool,
    interrupted: bool,
}
fn re(pattern: &'static str) -> Regex {
    static CACHE: LazyLock<Mutex<HashMap<&'static str, Regex>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    CACHE
        .lock()
        .expect("regex cache poisoned")
        .entry(pattern)
        .or_insert_with(|| Regex::new(pattern).expect("static regex is valid"))
        .clone()
}
fn sub(pattern: &'static str, text: &str, replacement: &str) -> String {
    re(pattern).replace_all(text, replacement).into_owned()
}
impl MarkdownTextFilter {
    pub fn new(settings: MarkdownSettings) -> Self {
        Self {
            settings,
            ..Self::default()
        }
    }
    pub fn settings(&self) -> &MarkdownSettings {
        &self.settings
    }
    pub fn update_settings(&mut self, settings: impl IntoIterator<Item = (impl AsRef<str>, bool)>) {
        for (key, value) in settings {
            match key.as_ref() {
                "enable_text_filter" => self.settings.enable_text_filter = value,
                "filter_code" => self.settings.filter_code = value,
                "filter_tables" => self.settings.filter_tables = value,
                "filter_repeated_sequences" => self.settings.filter_repeated_sequences = value,
                _ => {}
            }
        }
    }
    pub fn handle_interruption(&mut self) {
        self.interrupted = true;
        self.in_code_block = false;
        self.in_table = false;
    }
    pub fn reset_interruption(&mut self) {
        self.interrupted = false;
    }
    pub fn filter(&mut self, text: &str) -> String {
        if !self.settings.enable_text_filter {
            return text.to_owned();
        }
        let mut text = sub(r"\n\s*\n(#{1,6}[ \t])", text, "\n$1");
        text = sub(r"(?m)^#{1,6}[ \t]+(.*?)(?:[ \t]+#+[ \t]*)?$", &text, "$1");
        text = sub(r"(?m)^\s*\n", &text, " ");
        text = remove_single_backticks(&text);
        if self.settings.filter_repeated_sequences {
            text = remove_repeated_sequences(&text);
        }
        text = sub(r"^(\d+\.)\s", &text, "§NUM§$1 ");
        text = re(r"(?m)^( +)|\s+$")
            .replace_all(&text, |c: &Captures<'_>| "§".repeat(c[0].chars().count()))
            .into_owned();
        text = sub(r"§\| ", &text, "| ");

        // Python-Markdown without the fenced-code extension recognizes balanced
        // backtick runs as inline code, even across newlines. Protect unmatched
        // runs from CommonMark's automatic fenced-code interpretation.
        if self.settings.filter_tables {
            text = normalize_table_separators(&text);
        }
        let (text_for_parser, protected) = protect_markdown_literals(&text);
        let text_for_parser = protect_paragraph_list_markers(&text_for_parser);
        let options = if self.settings.filter_tables {
            Options::ENABLE_TABLES
        } else {
            Options::empty()
        };
        let mut rendered = String::new();
        html::push_html(&mut rendered, Parser::new_ext(&text_for_parser, options));
        text = rendered.trim().to_owned();
        for (marker, literal) in protected.into_iter().rev() {
            text = text.replace(&marker, &literal);
        }
        if self.settings.filter_tables {
            text = self.remove_tables(&text);
        }
        text = sub(r"<[^<]+?>", &text, "");
        text = text
            .replace("&nbsp;", " ")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&");
        text = text.replace("**", "");
        text = sub(r"(^|\s)\*|\*($|\s)", &text, "$1$2");
        text = text.replace('|', "");
        text = sub(r"(?m)^\s*[-:]+\s*$", &text, "");
        if self.settings.filter_code {
            text = self.remove_code_blocks(&text);
        }
        text = text.replace("§NUM§", "").replace('§', " ");
        sub(r"https?://", &text, "")
    }
    pub fn remove_code_blocks(&mut self, text: &str) -> String {
        if self.interrupted {
            self.in_code_block = false;
            return text.to_owned();
        }
        let found = text.find("```");
        if self.in_code_block {
            return if let Some(start) = found {
                self.in_code_block = false;
                text[start + 3..].trim().to_owned()
            } else {
                String::new()
            };
        }
        let Some(start) = found else {
            return text.to_owned();
        };
        if start == 0 || text[..start].chars().all(char::is_whitespace) {
            self.in_code_block = true;
            return text[..start].trim().to_owned();
        }
        let parts: Vec<_> = text.split("```").collect();
        if parts.len() > 2 {
            format!("{} {}", parts[0], parts.last().unwrap())
                .trim()
                .to_owned()
        } else {
            self.in_code_block = true;
            parts[0].trim().to_owned()
        }
    }
    pub fn remove_tables(&mut self, text: &str) -> String {
        if self.interrupted {
            self.in_table = false;
            return text.to_owned();
        }
        let text = sub(r"(?si)<table>.*?</table>", text, "");
        if self.in_table {
            return if let Some(m) = re(r"(?si)^.*</table>").find(&text) {
                self.in_table = false;
                text[m.end()..].trim().to_owned()
            } else {
                String::new()
            };
        }
        if let Some(m) = re(r"(?si)<table>.*").find(&text) {
            self.in_table = true;
            return text[..m.start()].trim().to_owned();
        }
        text.trim().to_owned()
    }
}
fn remove_repeated_sequences(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        let mut count = 1;
        while chars.peek() == Some(&c) {
            chars.next();
            count += 1;
        }
        if c.is_whitespace() || count < 5 {
            out.extend(std::iter::repeat_n(c, count));
        }
    }
    out
}
fn remove_single_backticks(text: &str) -> String {
    let mut out = String::new();
    let mut pos = 0;
    while pos < text.len() {
        let c = text[pos..].chars().next().unwrap();
        if c == '`' && (pos == 0 || text.as_bytes()[pos - 1] != b'`') {
            if let Some(relative) = text[pos + 1..].find('`') {
                let end = pos + 1 + relative;
                let content = &text[pos + 1..end];
                if !content.is_empty()
                    && !content.contains('\n')
                    && text.as_bytes().get(end + 1) != Some(&b'`')
                {
                    out.push_str(content);
                    pos = end + 1;
                    continue;
                }
            }
        }
        out.push(c);
        pos += c.len_utf8();
    }
    out
}
fn protect_markdown_literals(text: &str) -> (String, Vec<(String, String)>) {
    let mut literals = vec![];
    let mut prefix = "\u{e000}PC".to_owned();
    while text.contains(&prefix) {
        prefix.push('P');
    }
    let mut protect = |literal: String| {
        let marker = format!("{prefix}{}\u{e001}", literals.len());
        literals.push((marker.clone(), literal));
        marker
    };
    let text = re(r"&(?:#[xX][0-9a-fA-F]+|#\d+|[a-zA-Z][a-zA-Z0-9]*);")
        .replace_all(text, |c: &Captures<'_>| protect(c[0].to_owned()))
        .into_owned();
    let text = re(r"<(?:mailto:)?([A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+)>")
        .replace_all(&text, |c: &Captures<'_>| {
            protect(c[1].chars().map(|c| format!("&#{};", c as u32)).collect())
        })
        .into_owned();
    let mut output = String::new();
    let mut pos = 0;
    while pos < text.len() {
        let c = text[pos..].chars().next().unwrap();
        if c != '`' {
            output.push(c);
            pos += c.len_utf8();
            continue;
        }
        let count = text[pos..].bytes().take_while(|&b| b == b'`').count();
        let marker = "`".repeat(count);
        let content_start = pos + count;
        let mut search = content_start;
        let mut close = None;
        while let Some(found) = text[search..].find(&marker).map(|i| search + i) {
            let after = found + count;
            if text.as_bytes().get(after) != Some(&b'`')
                && (found == 0 || text.as_bytes()[found - 1] != b'`')
            {
                close = Some(found);
                break;
            }
            search = after;
        }
        if let Some(end) = close {
            let content = text[content_start..end]
                .trim()
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            output.push_str(&protect(format!("<code>{content}</code>")));
            pos = end + count;
        } else {
            output.push_str(&protect(marker));
            pos = content_start;
        }
    }
    (output, literals)
}

fn normalize_table_separators(text: &str) -> String {
    let mut previous_was_header = false;
    text.split('\n')
        .map(|line| {
            let is_separator = previous_was_header
                && line.contains('|')
                && line.chars().all(|c| "|:- \t".contains(c));
            previous_was_header = line.contains('|');
            if is_separator {
                let cells: Vec<_> = line
                    .trim()
                    .strip_prefix('|')
                    .unwrap_or(line.trim())
                    .strip_suffix('|')
                    .unwrap_or(line.trim())
                    .split('|')
                    .map(|cell| {
                        let cell = cell.trim();
                        format!(
                            "{}---{}",
                            if cell.starts_with(':') { ":" } else { "" },
                            if cell.ends_with(':') { ":" } else { "" }
                        )
                    })
                    .collect();
                format!("|{}|", cells.join("|"))
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn protect_paragraph_list_markers(text: &str) -> String {
    let mut in_list = false;
    text.split('\n')
        .enumerate()
        .map(|(index, line)| {
            let marker = re(r"^(?:[-+*] |[0-9]+[.)] )").find(line);
            if marker.is_some() {
                if index == 0 || in_list {
                    in_list = true;
                    return line.to_owned();
                }
                if line.as_bytes()[0].is_ascii_digit() {
                    return re(r"^([0-9]+)([.)])").replace(line, "$1\\$2").into_owned();
                }
                return format!("\\{line}");
            }
            in_list = false;
            line.to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n")
}
