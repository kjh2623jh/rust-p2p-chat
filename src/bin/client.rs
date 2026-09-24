use p2p_chat::{
    app::ChatApp,
    network::{NetworkCommand, NetworkEvent, client::NetworkClient},
};
use tokio::{runtime::Builder, sync::mpsc};

fn main() -> eframe::Result<()> {
    let (command_tx, command_rx) = mpsc::channel::<NetworkCommand>(32);
    let (event_tx, event_rx) = mpsc::channel::<NetworkEvent>(32);

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create Tokio runtime");

    std::thread::Builder::new()
        .name("network-runtime".to_string())
        .spawn(move || {
            runtime.block_on(NetworkClient::new(command_rx, event_tx).run());
        })
        .expect("failed to start network thread");

    let options = eframe::NativeOptions::default();

    eframe::run_native(
        "P2P Chat",
        options,
        Box::new(move |cc| Ok(Box::new(ChatApp::new(cc, command_tx, event_rx)))),
    )
}
