use tokio::sync::mpsc;

use crate::models::TestEvent;
use crate::sender::EventSender;

pub async fn handle_events(
    mut rx: mpsc::Receiver<TestEvent>,
    mut senders: Vec<Box<dyn EventSender + Send>>,
) {
    while let Some(event) = rx.recv().await {
        for sender in &mut senders {
            if let Err(e) = sender.send(event.clone()).await {
                tracing::error!(error = %e, "Dispatcher: send error");
            }
        }
    }
    tracing::info!("Dispatcher: exiting");
}
