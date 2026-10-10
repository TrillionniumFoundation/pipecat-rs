//! Feature-gated executable entry point for hosted native release evaluation.
#[path = "../../examples/native_text_bot.rs"]
mod application;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    application::main()
}
