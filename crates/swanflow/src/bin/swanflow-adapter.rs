use std::io::{self, Read, Write};

use swanflow::adapter::{handle_request, protocol::AdapterMessage, protocol::AdapterRequest};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;

    let request: AdapterRequest = serde_json::from_str(&input)?;
    let messages = match handle_request(request).await {
        Ok(messages) => messages,
        Err(error) => vec![AdapterMessage::Error {
            request_id: None,
            code: "adapter_request_failed".to_owned(),
            message: error.to_string(),
        }],
    };

    let mut stdout = io::stdout().lock();
    for message in messages {
        writeln!(stdout, "{}", serde_json::to_string(&message)?)?;
    }

    Ok(())
}
