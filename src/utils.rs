pub struct DropOwner {
    _receiver: tokio::sync::mpsc::Receiver<()>,
}

#[derive(Clone)]
pub struct DropWatcher {
    sender: tokio::sync::mpsc::Sender<()>,
}

impl DropWatcher {
    pub async fn dropped(&mut self) {
        self.sender.closed().await;
    }
}

pub fn drop_watcher() -> (DropOwner, DropWatcher) {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    (DropOwner { _receiver: receiver }, DropWatcher { sender })
}
