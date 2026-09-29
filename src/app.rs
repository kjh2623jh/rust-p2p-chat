use std::{sync::Arc, time::Duration};

use eframe::egui::{self, Align, Color32, CornerRadius, Layout, Margin, RichText, Stroke};
use tokio::sync::mpsc::{self, error::TrySendError};
use zeroize::Zeroize;

use crate::network::{NetworkCommand, NetworkEvent};
use crate::protocol::{InviteCode, MAX_MESSAGE_BYTES, MAX_MESSAGE_CHARS};

const BACKGROUND: Color32 = Color32::from_rgb(7, 7, 8);
const TOP_BAR: Color32 = Color32::from_rgb(18, 18, 18);
const SURFACE: Color32 = Color32::from_rgb(20, 20, 20);
const SURFACE_RAISED: Color32 = Color32::from_rgb(30, 30, 30);
const BORDER: Color32 = Color32::from_rgb(54, 54, 54);
const TEXT: Color32 = Color32::from_rgb(232, 232, 232);
const MUTED: Color32 = Color32::from_rgb(142, 142, 142);
const PRIMARY: Color32 = Color32::from_rgb(22, 119, 255);
const PRIMARY_HOVER: Color32 = Color32::from_rgb(64, 150, 255);
const SUCCESS: Color32 = Color32::from_rgb(82, 196, 26);
const WARNING: Color32 = Color32::from_rgb(250, 173, 20);
const ERROR: Color32 = Color32::from_rgb(255, 77, 79);
const MAX_HISTORY: usize = 500;

#[derive(Clone, Copy)]
enum NoticeTone {
    Info,
    Success,
    Warning,
    Error,
}

#[derive(Clone)]
struct Notice {
    tone: NoticeTone,
    title: String,
    detail: String,
}

#[derive(Clone, Copy)]
enum MessageAuthor {
    Me,
    Peer,
}

struct ChatMessage {
    id: Option<u64>,
    author: MessageAuthor,
    text: String,
    delivery: DeliveryState,
}

#[derive(Clone)]
enum DeliveryState {
    Pending,
    Delivered,
    Failed(String),
    Received,
}

pub struct ChatApp {
    command_tx: mpsc::Sender<NetworkCommand>,
    event_rx: mpsc::Receiver<NetworkEvent>,
    server_connected: bool,
    room_request_pending: bool,
    current_room: Option<String>,
    current_invite: Option<String>,
    peer_connected: bool,
    room_code: String,
    message_input: String,
    messages: Vec<ChatMessage>,
    notice: Option<Notice>,
    last_network_error: Option<String>,
    focus_room_code: bool,
    focus_message: bool,
}

impl ChatApp {
    pub fn new(
        cc: &eframe::CreationContext<'_>,
        command_tx: mpsc::Sender<NetworkCommand>,
        event_rx: mpsc::Receiver<NetworkEvent>,
    ) -> Self {
        setup_fonts(&cc.egui_ctx);
        setup_style(&cc.egui_ctx);

        Self {
            command_tx,
            event_rx,
            server_connected: false,
            room_request_pending: false,
            current_room: None,
            current_invite: None,
            peer_connected: false,
            room_code: String::new(),
            message_input: String::new(),
            messages: Vec::new(),
            notice: Some(Notice {
                tone: NoticeTone::Info,
                title: "서버에 연결하는 중입니다".to_string(),
                detail: "잠시만 기다려 주세요.".to_string(),
            }),
            last_network_error: None,
            focus_room_code: false,
            focus_message: false,
        }
    }

    fn process_network_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            if !matches!(
                &event,
                NetworkEvent::Error(_) | NetworkEvent::ServerDisconnected
            ) {
                self.last_network_error = None;
            }

            match event {
                NetworkEvent::ServerConnected => {
                    self.server_connected = true;
                    self.room_request_pending = false;
                    self.last_network_error = None;
                    self.set_notice(
                        NoticeTone::Success,
                        "서버 연결 완료",
                        "새 방을 만들거나 방 코드로 입장할 수 있습니다.",
                    );
                    self.focus_room_code = true;
                }
                NetworkEvent::ServerDisconnected => {
                    self.reset_room_state();
                    self.server_connected = false;
                    let detail = self
                        .last_network_error
                        .take()
                        .map(|error| {
                            format!(
                                "원인: {error}\n3초마다 자동으로 다시 연결합니다. 연결되면 바로 알려드릴게요."
                            )
                        })
                        .unwrap_or_else(|| {
                            "3초마다 자동으로 다시 연결합니다. 연결되면 바로 알려드릴게요."
                                .to_string()
                        });
                    self.set_notice(NoticeTone::Error, "서버 연결이 끊어졌습니다", detail);
                }
                NetworkEvent::RoomCreated {
                    invite,
                    fingerprint,
                } => {
                    self.enter_room(fingerprint.clone(), Some(invite));
                    self.set_notice(
                        NoticeTone::Info,
                        format!("대화 {fingerprint}을 만들었습니다"),
                        "보안 초대 코드를 복사해 상대방에게 전달해 주세요.",
                    );
                }
                NetworkEvent::JoinedRoom { fingerprint } => {
                    self.enter_room(fingerprint.clone(), None);
                    self.set_notice(
                        NoticeTone::Info,
                        format!("대화 {fingerprint}에 입장했습니다"),
                        "초대 코드를 확인하고 종단간 암호화 연결을 설정하고 있습니다.",
                    );
                }
                NetworkEvent::RoomFull => {
                    self.room_request_pending = false;
                    self.set_notice(
                        NoticeTone::Warning,
                        "입장할 수 없는 방입니다",
                        "이미 두 명이 대화 중입니다. 다른 방 코드를 입력해 주세요.",
                    );
                    self.focus_room_code = true;
                }
                NetworkEvent::RoomNotFound => {
                    self.room_request_pending = false;
                    self.set_notice(
                        NoticeTone::Error,
                        "방을 찾지 못했습니다",
                        "방 코드가 정확한지 확인한 뒤 다시 시도해 주세요.",
                    );
                    self.focus_room_code = true;
                }
                NetworkEvent::AlreadyInRoom => {
                    self.room_request_pending = false;

                    if self.send_command(NetworkCommand::LeaveRoom) {
                        self.reset_room_state();
                        self.set_notice(
                            NoticeTone::Warning,
                            "이전 방 상태를 정리했습니다",
                            "이제 새 방을 만들거나 다른 방에 입장할 수 있습니다.",
                        );
                        self.focus_room_code = true;
                    }
                }
                NetworkEvent::PeerConnected => {
                    if self.current_room.is_some() {
                        self.peer_connected = true;
                        self.set_notice(
                            NoticeTone::Success,
                            "P2P 연결 완료",
                            "메시지는 서버를 거치지 않고 상대방에게 직접 전송됩니다.",
                        );
                        self.focus_message = true;
                    }
                }
                NetworkEvent::PeerDisconnected => {
                    self.reset_room_state();
                    self.set_notice(
                        NoticeTone::Warning,
                        "상대방과 연결이 종료되었습니다",
                        "기존 방을 안전하게 정리했습니다. 새 대화를 바로 시작할 수 있습니다.",
                    );
                    self.focus_room_code = true;
                }
                NetworkEvent::P2pFailed => {
                    self.reset_room_state();
                    self.set_notice(
                        NoticeTone::Error,
                        "상대방과 직접 연결하지 못했습니다",
                        "네트워크 환경을 확인하고 새 방에서 다시 시도해 주세요.",
                    );
                    self.focus_room_code = true;
                }
                NetworkEvent::MessagePending { id, text } => {
                    if self.current_room.is_some() && self.peer_connected {
                        self.messages.push(ChatMessage {
                            id: Some(id),
                            author: MessageAuthor::Me,
                            text,
                            delivery: DeliveryState::Pending,
                        });
                        self.trim_history();
                    }
                }
                NetworkEvent::MessageDelivered(id) => {
                    if let Some(message) = self
                        .messages
                        .iter_mut()
                        .find(|message| message.id == Some(id))
                    {
                        message.delivery = DeliveryState::Delivered;
                    }
                }
                NetworkEvent::MessageFailed { id, reason } => {
                    if let Some(message) = self
                        .messages
                        .iter_mut()
                        .find(|message| message.id == Some(id))
                    {
                        message.delivery = DeliveryState::Failed(reason.clone());
                    }
                    self.set_notice(NoticeTone::Warning, "메시지를 전달하지 못했습니다", reason);
                }
                NetworkEvent::MessageReceived(message) => {
                    if self.current_room.is_some() && self.peer_connected {
                        self.messages.push(ChatMessage {
                            id: None,
                            author: MessageAuthor::Peer,
                            text: message,
                            delivery: DeliveryState::Received,
                        });
                        self.trim_history();
                    }
                }
                NetworkEvent::Error(error) => {
                    self.room_request_pending = false;
                    let error = error.to_string();
                    self.last_network_error = Some(error.clone());
                    self.set_notice(
                        NoticeTone::Error,
                        "네트워크 작업 중 오류가 발생했습니다",
                        error,
                    );
                }
            }
        }
    }

    fn enter_room(&mut self, fingerprint: String, invite: Option<String>) {
        self.current_room = Some(fingerprint);
        self.current_invite = invite;
        self.room_code.zeroize();
        self.room_request_pending = false;
        self.peer_connected = false;
        self.messages.clear();
        self.message_input.clear();
    }

    fn reset_room_state(&mut self) {
        self.current_room = None;
        if let Some(mut invite) = self.current_invite.take() {
            invite.zeroize();
        }
        self.room_code.zeroize();
        self.room_request_pending = false;
        self.peer_connected = false;
        self.message_input.clear();
    }

    fn trim_history(&mut self) {
        if self.messages.len() > MAX_HISTORY {
            self.messages.drain(..self.messages.len() - MAX_HISTORY);
        }
    }

    fn set_notice(
        &mut self,
        tone: NoticeTone,
        title: impl Into<String>,
        detail: impl Into<String>,
    ) {
        self.notice = Some(Notice {
            tone,
            title: title.into(),
            detail: detail.into(),
        });
    }

    fn send_command(&mut self, command: NetworkCommand) -> bool {
        match self.command_tx.try_send(command) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                self.set_notice(
                    NoticeTone::Warning,
                    "요청이 잠시 밀려 있습니다",
                    "잠시 후 다시 시도해 주세요.",
                );
                false
            }
            Err(TrySendError::Closed(_)) => {
                self.server_connected = false;
                self.reset_room_state();
                self.set_notice(
                    NoticeTone::Error,
                    "네트워크 작업이 종료되었습니다",
                    "앱을 다시 실행해 연결을 복구해 주세요.",
                );
                false
            }
        }
    }

    fn create_room(&mut self) {
        if self.send_command(NetworkCommand::CreateRoom) {
            self.room_request_pending = true;
            self.set_notice(
                NoticeTone::Info,
                "방을 만드는 중입니다",
                "서버의 응답을 기다리고 있습니다.",
            );
        }
    }

    fn join_room(&mut self) {
        let invite = match self.room_code.trim().parse::<InviteCode>() {
            Ok(invite) => invite,
            Err(()) => {
                self.set_notice(
                    NoticeTone::Warning,
                    "초대 코드를 확인해 주세요",
                    "P2P2-로 시작하는 전체 보안 초대 코드를 붙여넣어 주세요.",
                );
                self.focus_room_code = true;
                return;
            }
        };
        let fingerprint = invite.fingerprint();

        if self.send_command(NetworkCommand::JoinRoom(invite)) {
            self.room_request_pending = true;
            self.set_notice(
                NoticeTone::Info,
                format!("대화 {fingerprint}에 입장하는 중입니다"),
                "보안 연결을 준비하고 있습니다.",
            );
        }
    }

    fn leave_room(&mut self) {
        if self.send_command(NetworkCommand::LeaveRoom) {
            self.reset_room_state();
            self.set_notice(
                NoticeTone::Info,
                "방에서 나왔습니다",
                "새 방을 만들거나 다른 방에 입장할 수 있습니다.",
            );
            self.focus_room_code = true;
        }
    }

    fn send_message(&mut self) {
        let message = self.message_input.trim().to_string();
        let character_count = message.chars().count();

        if message.is_empty() {
            return;
        }

        if character_count > MAX_MESSAGE_CHARS || message.len() > MAX_MESSAGE_BYTES {
            self.set_notice(
                NoticeTone::Warning,
                "메시지가 너무 깁니다",
                format!(
                    "한 번에 {MAX_MESSAGE_CHARS}자, {MAX_MESSAGE_BYTES}바이트까지 보낼 수 있습니다."
                ),
            );
            self.focus_message = true;
            return;
        }

        if self.send_command(NetworkCommand::SendMessage(message)) {
            self.message_input.clear();
            self.focus_message = true;
        }
    }

    fn render_header(&mut self, ui: &mut egui::Ui) {
        egui::Frame::new()
            .fill(TOP_BAR)
            .stroke(Stroke::new(1.0, BORDER))
            .inner_margin(Margin::symmetric(20, 8))
            .show(ui, |ui| {
                ui.set_height(30.0);
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("P2P")
                            .size(15.0)
                            .strong()
                            .color(PRIMARY_HOVER),
                    );
                    ui.label(RichText::new("Chat").size(15.0).strong().color(TEXT));

                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let (label, color) = if self.server_connected {
                            ("서버 연결됨", SUCCESS)
                        } else {
                            ("서버 연결 중", WARNING)
                        };

                        status_pill(ui, label, color);
                    });
                });
            });
    }

    fn render_page_header(&mut self, ui: &mut egui::Ui) {
        let room_fingerprint = self.current_room.clone();
        let invite = self.current_invite.clone();

        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.label(
                    RichText::new(if self.current_room.is_some() {
                        "1:1 대화"
                    } else {
                        "대화 시작"
                    })
                    .size(22.0)
                    .strong()
                    .color(TEXT),
                );
                ui.add_space(3.0);
                ui.label(
                    RichText::new(if self.current_room.is_some() {
                        "종단간 암호화된 메시지를 상대방과 직접 주고받습니다."
                    } else {
                        "새 대화를 만들거나 받은 보안 초대 코드로 입장하세요."
                    })
                    .size(11.0)
                    .color(MUTED),
                );
            });

            ui.with_layout(Layout::right_to_left(Align::BOTTOM), |ui| {
                if room_fingerprint.is_some()
                    && ui
                        .add(
                            egui::Button::new(RichText::new("방 나가기").color(ERROR))
                                .fill(Color32::TRANSPARENT)
                                .stroke(Stroke::new(1.0, ERROR.gamma_multiply(0.55)))
                                .corner_radius(CornerRadius::same(6)),
                        )
                        .clicked()
                {
                    self.leave_room();
                    return;
                }

                if let Some(invite) = invite
                    && ui
                        .add(
                            egui::Button::new(
                                RichText::new("보안 초대 코드 복사").strong().color(TEXT),
                            )
                            .fill(SURFACE)
                            .stroke(Stroke::new(1.0, BORDER))
                            .corner_radius(CornerRadius::same(6)),
                        )
                        .on_hover_text("전체 초대 코드를 클립보드에 복사")
                        .clicked()
                {
                    ui.ctx().copy_text(invite);
                    self.set_notice(
                        NoticeTone::Success,
                        "보안 초대 코드를 복사했습니다",
                        "신뢰할 수 있는 방법으로 상대방에게 전달해 주세요.",
                    );
                }

                if self.peer_connected {
                    status_pill(ui, "상대방 연결됨", SUCCESS);
                }

                if let Some(fingerprint) = room_fingerprint {
                    status_pill(ui, &format!("대화 {fingerprint}"), PRIMARY_HOVER);
                }
            });
        });
    }

    fn render_notice(&mut self, ui: &mut egui::Ui) {
        let Some(notice) = self.notice.clone() else {
            return;
        };

        let color = tone_color(notice.tone);
        let fill = color.gamma_multiply(0.12);

        egui::Frame::new()
            .fill(fill)
            .stroke(Stroke::new(1.0, color.gamma_multiply(0.55)))
            .corner_radius(CornerRadius::same(6))
            .inner_margin(Margin::same(12))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(RichText::new(notice.title).strong().color(TEXT));
                        if !notice.detail.is_empty() {
                            ui.label(RichText::new(notice.detail).size(12.0).color(MUTED));
                        }
                    });

                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if ui
                            .add(
                                egui::Button::new(RichText::new("닫기").size(11.0).color(MUTED))
                                    .frame(false),
                            )
                            .on_hover_text("알림 닫기")
                            .clicked()
                        {
                            self.notice = None;
                        }
                    });
                });
            });
    }

    fn render_lobby(&mut self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical()
            .id_salt("lobby")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.vertical_centered(|ui| {
                    ui.set_max_width(680.0);
                    egui::Frame::new()
                        .fill(SURFACE)
                        .stroke(Stroke::new(1.0, BORDER))
                        .corner_radius(CornerRadius::same(7))
                        .inner_margin(Margin::same(20))
                        .show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        let enabled = self.server_connected && !self.room_request_pending;

                        ui.label(RichText::new("새 방 만들기").size(15.0).strong());
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new("추측하기 어려운 일회성 보안 초대 코드를 만듭니다.")
                                .size(11.0)
                                .color(MUTED),
                        );
                        ui.add_space(10.0);

                        let create_response = ui.add_enabled(
                            enabled,
                            egui::Button::new(
                                RichText::new(if self.room_request_pending {
                                    "처리 중..."
                                } else {
                                    "새 방 만들기"
                                })
                                .strong()
                                    .color(Color32::WHITE),
                            )
                            .fill(PRIMARY)
                            .stroke(Stroke::NONE)
                            .corner_radius(CornerRadius::same(6))
                            .min_size(egui::vec2(150.0, 42.0)),
                        );

                        if create_response.clicked() {
                            self.create_room();
                        }

                        ui.add_space(20.0);
                        ui.separator();
                        ui.add_space(16.0);
                        ui.label(RichText::new("초대 코드로 입장").size(15.0).strong());
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new("상대방에게 받은 전체 코드를 붙여넣으세요. Enter로 바로 입장할 수 있습니다.")
                                .size(11.0)
                                .color(MUTED),
                        );
                        ui.add_space(10.0);

                        ui.horizontal(|ui| {
                            let join_button_width = 88.0;
                            let input_width =
                                (ui.available_width() - join_button_width - 8.0).max(120.0);
                            let response = ui.add_enabled(
                                enabled,
                                egui::TextEdit::singleline(&mut self.room_code)
                                    .hint_text("P2P2-로 시작하는 보안 초대 코드")
                                    .char_limit(128)
                                    .desired_width(input_width)
                                    .font(egui::TextStyle::Monospace)
                                    .text_color(TEXT)
                                    .background_color(SURFACE_RAISED)
                                    .margin(Margin::symmetric(12, 10))
                                    .vertical_align(Align::Center),
                            );

                            if self.focus_room_code && enabled {
                                response.request_focus();
                                self.focus_room_code = false;
                            }

                            let enter_pressed = response.has_focus()
                                && ui.input(|input| input.key_pressed(egui::Key::Enter));
                            let can_join = enabled && self.room_code.trim().parse::<InviteCode>().is_ok();
                            let join_clicked = ui
                                .add_enabled(
                                    can_join,
                                    egui::Button::new(RichText::new("입장").strong())
                                        .fill(SURFACE_RAISED)
                                        .stroke(Stroke::new(1.0, BORDER))
                                        .corner_radius(CornerRadius::same(6))
                                        .min_size(egui::vec2(join_button_width, 42.0)),
                                )
                                .clicked();

                            if (enter_pressed && can_join) || join_clicked {
                                self.join_room();
                            }
                        });

                        ui.add_space(16.0);
                        ui.label(
                            RichText::new("개인정보 안내: 직접 연결 방식이므로 대화 상대는 회원님의 공개 IP 주소를 확인할 수 있습니다. 메시지 내용과 초대 비밀은 서버에 저장되지 않습니다.")
                                .size(11.0)
                                .color(MUTED),
                        );
                    });
                });
            });
    }

    fn render_chat(&mut self, ui: &mut egui::Ui) {
        let composer_height = 58.0;
        let gap = 12.0;
        let message_area_height = (ui.available_height() - composer_height - gap).max(80.0);

        egui::Frame::new()
            .fill(SURFACE)
            .stroke(Stroke::new(1.0, BORDER))
            .corner_radius(CornerRadius::same(7))
            .inner_margin(Margin::same(12))
            .show(ui, |ui| {
                ui.set_height(message_area_height - 24.0);
                ui.horizontal(|ui| {
                    ui.label(RichText::new("메시지").size(15.0).strong());
                    egui::Frame::new()
                        .fill(SURFACE_RAISED)
                        .corner_radius(CornerRadius::same(4))
                        .inner_margin(Margin::symmetric(7, 3))
                        .show(ui, |ui| {
                            ui.label(
                                RichText::new(self.messages.len().to_string())
                                    .size(10.0)
                                    .color(MUTED),
                            );
                        });

                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let (label, color) = if self.peer_connected {
                            ("상대방 연결됨", SUCCESS)
                        } else {
                            ("상대방 기다리는 중", WARNING)
                        };
                        ui.label(RichText::new(label).size(10.0).color(color));
                    });
                });
                ui.add_space(4.0);
                ui.separator();
                ui.add_space(4.0);

                egui::ScrollArea::vertical()
                    .id_salt("messages")
                    .auto_shrink([false, false])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        ui.set_width(ui.available_width());

                        if self.messages.is_empty() {
                            ui.add_space((ui.available_height() * 0.35).max(18.0));
                            ui.vertical_centered(|ui| {
                                let (title, detail) = if self.peer_connected {
                                    ("연결되었습니다", "첫 메시지를 보내 대화를 시작해 보세요.")
                                } else {
                                    (
                                        "상대방을 기다리고 있습니다",
                                        "위의 보안 초대 코드를 공유해 주세요.",
                                    )
                                };

                                ui.label(RichText::new(title).size(17.0).strong().color(TEXT));
                                ui.add_space(4.0);
                                ui.label(RichText::new(detail).color(MUTED));
                            });
                        } else {
                            let max_bubble_width = (ui.available_width() * 0.72).max(180.0);

                            for message in &self.messages {
                                render_message_bubble(ui, message, max_bubble_width);
                                ui.add_space(6.0);
                            }
                        }
                    });
            });

        ui.add_space(gap);

        egui::Frame::new()
            .fill(SURFACE)
            .stroke(Stroke::new(1.0, BORDER))
            .corner_radius(CornerRadius::same(7))
            .inner_margin(Margin::same(8))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    let send_button_width = 84.0;
                    let input_width = (ui.available_width() - send_button_width - 8.0).max(120.0);
                    let response = ui.add_enabled(
                        self.peer_connected,
                        egui::TextEdit::singleline(&mut self.message_input)
                            .hint_text(if self.peer_connected {
                                "메시지를 입력하세요"
                            } else {
                                "상대방과 연결되면 메시지를 보낼 수 있습니다"
                            })
                            .desired_width(input_width)
                            .text_color(TEXT)
                            .background_color(SURFACE_RAISED)
                            .margin(Margin::symmetric(12, 10))
                            .vertical_align(Align::Center),
                    );

                    if self.focus_message && self.peer_connected {
                        response.request_focus();
                        self.focus_message = false;
                    }

                    let enter_pressed = response.has_focus()
                        && ui.input(|input| input.key_pressed(egui::Key::Enter));
                    let can_send = self.peer_connected && !self.message_input.trim().is_empty();
                    let send_clicked = ui
                        .add_enabled(
                            can_send,
                            egui::Button::new(RichText::new("전송").strong().color(Color32::WHITE))
                                .fill(PRIMARY)
                                .stroke(Stroke::NONE)
                                .corner_radius(CornerRadius::same(6))
                                .min_size(egui::vec2(send_button_width, 42.0)),
                        )
                        .clicked();

                    if (enter_pressed && can_send) || send_clicked {
                        self.send_message();
                    }
                });
            });
    }
}

impl Drop for ChatApp {
    fn drop(&mut self) {
        self.room_code.zeroize();
        if let Some(invite) = &mut self.current_invite {
            invite.zeroize();
        }
    }
}

fn render_message_bubble(ui: &mut egui::Ui, message: &ChatMessage, max_width: f32) {
    let (layout, fill, label) = match message.author {
        MessageAuthor::Me => (Layout::right_to_left(Align::TOP), PRIMARY, "나"),
        MessageAuthor::Peer => (Layout::left_to_right(Align::TOP), SURFACE_RAISED, "상대방"),
    };

    ui.with_layout(layout, |ui| {
        ui.vertical(|ui| {
            ui.label(RichText::new(label).size(10.0).color(MUTED));
            egui::Frame::new()
                .fill(fill)
                .corner_radius(CornerRadius::same(7))
                .inner_margin(Margin::symmetric(12, 9))
                .show(ui, |ui| {
                    ui.set_max_width(max_width);
                    ui.label(RichText::new(&message.text).color(Color32::WHITE));
                });
            if matches!(message.author, MessageAuthor::Me) {
                let (status, color) = match &message.delivery {
                    DeliveryState::Pending => ("전송 중", MUTED),
                    DeliveryState::Delivered => ("전달됨", SUCCESS),
                    DeliveryState::Failed(_) => ("전송 실패", ERROR),
                    DeliveryState::Received => ("", MUTED),
                };
                let response = ui.label(RichText::new(status).size(9.0).color(color));
                if let DeliveryState::Failed(reason) = &message.delivery {
                    response.on_hover_text(reason);
                }
            }
        });
    });
}

fn status_pill(ui: &mut egui::Ui, label: &str, color: Color32) {
    egui::Frame::new()
        .fill(color.gamma_multiply(0.12))
        .stroke(Stroke::new(1.0, color.gamma_multiply(0.45)))
        .corner_radius(CornerRadius::same(6))
        .inner_margin(Margin::symmetric(10, 6))
        .show(ui, |ui| {
            ui.label(RichText::new(label).size(11.0).color(color));
        });
}

fn tone_color(tone: NoticeTone) -> Color32 {
    match tone {
        NoticeTone::Info => PRIMARY_HOVER,
        NoticeTone::Success => SUCCESS,
        NoticeTone::Warning => WARNING,
        NoticeTone::Error => ERROR,
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
        .expect("proportional font family should exist")
        .insert(0, "korean".to_owned());

    fonts
        .families
        .get_mut(&egui::FontFamily::Monospace)
        .expect("monospace font family should exist")
        .push("korean".to_owned());

    ctx.set_fonts(fonts);
}

fn setup_style(ctx: &egui::Context) {
    ctx.set_theme(egui::Theme::Dark);
    ctx.style_mut_of(egui::Theme::Dark, |style| {
        style.spacing.item_spacing = egui::vec2(8.0, 8.0);
        style.spacing.button_padding = egui::vec2(12.0, 8.0);
        style.spacing.interact_size.y = 38.0;

        style.visuals.panel_fill = BACKGROUND;
        style.visuals.window_fill = BACKGROUND;
        style.visuals.extreme_bg_color = SURFACE_RAISED;
        style.visuals.faint_bg_color = SURFACE;
        style.visuals.override_text_color = Some(TEXT);
        style.visuals.selection.bg_fill = PRIMARY;
        style.visuals.hyperlink_color = PRIMARY_HOVER;

        style.visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
        style.visuals.widgets.inactive.bg_fill = SURFACE_RAISED;
        style.visuals.widgets.inactive.weak_bg_fill = SURFACE_RAISED;
        style.visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, BORDER);
        style.visuals.widgets.inactive.corner_radius = CornerRadius::same(8);
        style.visuals.widgets.hovered.bg_fill = Color32::from_rgb(38, 38, 38);
        style.visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(38, 38, 38);
        style.visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, PRIMARY_HOVER);
        style.visuals.widgets.hovered.corner_radius = CornerRadius::same(8);
        style.visuals.widgets.active.bg_fill = PRIMARY;
        style.visuals.widgets.active.weak_bg_fill = PRIMARY;
        style.visuals.widgets.active.bg_stroke = Stroke::new(1.0, PRIMARY_HOVER);
        style.visuals.widgets.active.corner_radius = CornerRadius::same(8);
    });
}

impl eframe::App for ChatApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.process_network_events();
        ui.ctx().request_repaint_after(Duration::from_millis(100));
        ui.set_min_size(ui.available_size());

        egui::Frame::new().fill(BACKGROUND).show(ui, |ui| {
            ui.set_min_size(ui.available_size());
            self.render_header(ui);

            let content_size = ui.available_size();
            egui::Frame::new()
                .fill(BACKGROUND)
                .inner_margin(Margin::same(24))
                .show(ui, |ui| {
                    ui.set_min_size(egui::vec2(
                        (content_size.x - 48.0).max(360.0),
                        (content_size.y - 48.0).max(360.0),
                    ));
                    ui.set_max_width((content_size.x - 48.0).max(360.0));

                    self.render_page_header(ui);
                    ui.add_space(12.0);
                    self.render_notice(ui);
                    ui.add_space(14.0);

                    if self.current_room.is_some() {
                        self.render_chat(ui);
                    } else {
                        self.render_lobby(ui);
                    }
                });
        });
    }
}
