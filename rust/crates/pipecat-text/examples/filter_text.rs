//! Print a single Markdown fragment as spoken text.
use pipecat_text::MarkdownTextFilter;
fn main() {
    let mut filter = MarkdownTextFilter::default();
    for input in std::env::args().skip(1) {
        println!("{:?}", filter.filter(&input));
    }
}
