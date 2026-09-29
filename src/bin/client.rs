#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]

#[path = "client/renderer.rs"]
mod renderer;

use std::{error::Error, path::PathBuf};

use p2p_chat::{
    app::ChatApp,
    network::{NetworkCommand, NetworkEvent, client::NetworkClient},
};
use renderer::{Invocation, RendererMode};
use tokio::{runtime::Builder, sync::mpsc};

fn main() -> Result<(), Box<dyn Error>> {
    match renderer::parse_invocation()? {
        Invocation::Launcher => run_without_renderer_argument(),
        Invocation::Renderer { mode, ready_file } => run_renderer(mode, ready_file),
    }
}

#[cfg(target_os = "windows")]
fn run_without_renderer_argument() -> Result<(), Box<dyn Error>> {
    renderer::run_launcher()?;
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn run_without_renderer_argument() -> Result<(), Box<dyn Error>> {
    run_app(eframe::Renderer::Wgpu, None)?;
    Ok(())
}

fn run_renderer(mode: RendererMode, ready_file: Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    run_app(mode.eframe_renderer(), ready_file)?;
    Ok(())
}

fn run_app(selected_renderer: eframe::Renderer, ready_file: Option<PathBuf>) -> eframe::Result<()> {
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

    let options = eframe::NativeOptions {
        renderer: selected_renderer,
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([980.0, 720.0])
            .with_min_inner_size([720.0, 540.0]),
        ..Default::default()
    };

    eframe::run_native(
        "P2P Chat",
        options,
        Box::new(move |cc| {
            let app = ChatApp::new(cc, command_tx, event_rx);
            renderer::schedule_ready_signal(ready_file);
            Ok(Box::new(app))
        }),
    )
}
