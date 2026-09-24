use std::{sync::Arc, time::Duration};

use eframe::egui;
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::network::{NetworkCommand, NetworkEvent};

pub struct ChatApp {
    command_tx: mpsc::Sender<NetworkCommand>,
    event_rx: mpsc::Receiver<NetworkEvent>,
    server_connected: bool,
    current_room: Option<String>,
    peer_connected: bool,
    status_message: String,
    room_code: String,
    message_input: String,
    messages: Vec<String>,
}

impl ChatApp {
    pub fn new(
        cc: &eframe::CreationContext<'_>,
        command_tx: mpsc::Sender<NetworkCommand>,
        event_rx: mpsc::Receiver<NetworkEvent>,
    ) -> Self {
        setup_fonts(&cc.egui_ctx);

        Self {
            command_tx,
            event_rx,
            server_connected: false,
            current_room: None,
            peer_connected: false,
            status_message: "Signaling 서버에 연결 중...".to_string(),
            room_code: String::new(),
            message_input: String::new(),
            messages: Vec::new(),
        }
    }

    fn process_network_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                NetworkEvent::ServerConnected => {
                    self.server_connected = true;
                    self.status_message = "Signaling 서버에 연결되었습니다.".to_string();
                }
                NetworkEvent::ServerDisconnected => {
                    self.server_connected = false;
                    self.current_room = None;
                    self.peer_connected = false;
                    self.status_message = "Signaling 서버 연결이 끊겼습니다.".to_string();
                }
                NetworkEvent::RoomCreated(room_code) => {
                    self.current_room = Some(room_code.clone());
                    self.peer_connected = false;
                    self.messages.clear();
                    self.status_message =
                        format!("방 {room_code}을 만들었습니다. 상대를 기다립니다.");
                }
                NetworkEvent::JoinedRoom(room_code) => {
                    self.current_room = Some(room_code.clone());
                    self.peer_connected = false;
                    self.messages.clear();
                    self.status_message = format!("방 {room_code}에 입장했습니다.");
                }
                NetworkEvent::RoomFull => {
                    self.status_message = "방이 가득 찼습니다.".to_string();
                }
                NetworkEvent::RoomNotFound => {
                    self.status_message = "방을 찾을 수 없습니다.".to_string();
                }
                NetworkEvent::AlreadyInRoom => {
                    self.status_message = "이미 방에 들어가 있습니다.".to_string();
                }
                NetworkEvent::PeerConnected => {
                    if self.current_room.is_some() {
                        self.peer_connected = true;
                        self.status_message = "상대와 P2P로 연결되었습니다.".to_string();
                    }
                }
                NetworkEvent::PeerDisconnected => {
                    self.peer_connected = false;
                    self.status_message = "상대와의 P2P 연결이 끊겼습니다.".to_string();
                }
                NetworkEvent::P2pFailed => {
                    self.current_room = None;
                    self.peer_connected = false;
                    self.status_message =
                        "P2P 연결에 실패했습니다. 다른 방을 다시 시도할 수 있습니다.".to_string();
                }
                NetworkEvent::MessageSent(message) => {
                    if self.current_room.is_some() && self.peer_connected {
                        self.messages.push(format!("나: {message}"));
                    }
                }
                NetworkEvent::MessageReceived(message) => {
                    if self.current_room.is_some() && self.peer_connected {
                        self.messages.push(format!("상대: {message}"));
                    }
                }
                NetworkEvent::Error(error) => {
                    self.status_message = format!("네트워크 오류: {error}");
                }
            }
        }
    }

    fn send_command(&mut self, command: NetworkCommand) -> bool {
        match self.command_tx.try_send(command) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                self.status_message =
                    "네트워크 요청이 많습니다. 잠시 후 다시 시도하세요.".to_string();
                false
            }
            Err(TrySendError::Closed(_)) => {
                self.server_connected = false;
                self.status_message = "네트워크 작업이 종료되었습니다.".to_string();
                false
            }
        }
    }
}

fn setup_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();

    fonts.font_data.insert(
        "korean".to_owned(),
        Arc::new(egui::FontData::from_static(include_bytes!(
            "../assets/fonts/Pretendard-Regular.otf"
        ))),
    );

    fonts
        .families
        .get_mut(&egui::FontFamily::Proportional)
        .unwrap()
        .insert(0, "korean".to_owned());

    fonts
        .families
        .get_mut(&egui::FontFamily::Monospace)
        .unwrap()
        .push("korean".to_owned());

    ctx.set_fonts(fonts);
}

impl eframe::App for ChatApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.process_network_events();
        ui.ctx().request_repaint_after(Duration::from_millis(100));

        ui.heading("P2P Chat");
        ui.separator();

        let server_status = if self.server_connected {
            "연결됨"
        } else {
            "연결 안 됨"
        };
        ui.label(format!("서버 상태: {server_status}"));

        if let Some(room_code) = &self.current_room {
            ui.label(format!("현재 방: {room_code}"));
        }

        let peer_status = if self.peer_connected {
            "P2P 연결됨"
        } else {
            "P2P 연결 안 됨"
        };
        ui.label(format!("상대 상태: {peer_status}"));
        ui.label(&self.status_message);

        ui.add_space(10.0);

        let can_enter_room = self.server_connected && self.current_room.is_none();

        if ui
            .add_enabled(can_enter_room, egui::Button::new("방 만들기"))
            .clicked()
            && self.send_command(NetworkCommand::CreateRoom)
        {
            self.status_message = "방 생성 요청을 보냈습니다.".to_string();
        }

        ui.add_space(10.0);

        ui.horizontal(|ui| {
            ui.label("방 코드");
            ui.add_enabled(
                can_enter_room,
                egui::TextEdit::singleline(&mut self.room_code),
            );

            let can_join = can_enter_room && !self.room_code.trim().is_empty();
            if ui
                .add_enabled(can_join, egui::Button::new("입장"))
                .clicked()
            {
                let room_code = self.room_code.trim().to_uppercase();

                if self.send_command(NetworkCommand::JoinRoom(room_code)) {
                    self.status_message = "방 입장 요청을 보냈습니다.".to_string();
                }
            }
        });

        if self.current_room.is_some()
            && ui.button("방 나가기").clicked()
            && self.send_command(NetworkCommand::LeaveRoom)
        {
            self.current_room = None;
            self.peer_connected = false;
            self.messages.clear();
            self.status_message = "방에서 나갔습니다.".to_string();
        }

        ui.separator();

        egui::ScrollArea::vertical().show(ui, |ui| {
            for message in &self.messages {
                ui.label(message);
            }
        });

        ui.separator();

        ui.horizontal(|ui| {
            ui.add_enabled(
                self.peer_connected,
                egui::TextEdit::singleline(&mut self.message_input),
            );

            let can_send = self.peer_connected && !self.message_input.trim().is_empty();
            if ui
                .add_enabled(can_send, egui::Button::new("전송"))
                .clicked()
            {
                let message = self.message_input.trim().to_string();

                if self.send_command(NetworkCommand::SendMessage(message)) {
                    self.message_input.clear();
                }
            }
        });
    }
}
