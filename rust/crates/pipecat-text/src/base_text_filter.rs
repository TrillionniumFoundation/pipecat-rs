//! Object-safe native text-filter interface.
/// Pure text transformation and interruption hooks. Settings changes are
/// implementation-specific; for Markdown use `MarkdownTextFilter::update_settings`.
pub trait TextFilter: Send {
    fn filter(&mut self, text: &str) -> String;
    fn handle_interruption(&mut self) {}
    fn reset_interruption(&mut self) {}
}
impl TextFilter for crate::MarkdownTextFilter {
    fn filter(&mut self, text: &str) -> String {
        crate::MarkdownTextFilter::filter(self, text)
    }
    fn handle_interruption(&mut self) {
        crate::MarkdownTextFilter::handle_interruption(self);
    }
    fn reset_interruption(&mut self) {
        crate::MarkdownTextFilter::reset_interruption(self);
    }
}
