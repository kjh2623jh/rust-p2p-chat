use std::{
    env,
    ffi::OsString,
    fmt,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process::{self, Command, ExitStatus},
    str::FromStr,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const RENDERER_ARGUMENT: &str = "--renderer";
const READY_FILE_ARGUMENT: &str = "--renderer-ready-file";
const STARTUP_GRACE_PERIOD: Duration = Duration::from_secs(2);
const WINDOWS_RENDERER_PRIORITY: [RendererMode; 3] =
    [RendererMode::Dx12, RendererMode::Glow, RendererMode::Vulkan];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RendererMode {
    Dx12,
    Glow,
    Vulkan,
}

impl RendererMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dx12 => "dx12",
            Self::Glow => "glow",
            Self::Vulkan => "vulkan",
        }
    }

    pub fn eframe_renderer(self) -> eframe::Renderer {
        match self {
            Self::Dx12 | Self::Vulkan => eframe::Renderer::Wgpu,
            Self::Glow => eframe::Renderer::Glow,
        }
    }

    fn wgpu_backend(self) -> Option<&'static str> {
        match self {
            Self::Dx12 => Some("dx12"),
            Self::Glow => None,
            Self::Vulkan => Some("vulkan"),
        }
    }
}

impl fmt::Display for RendererMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RendererMode {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "dx12" => Ok(Self::Dx12),
            "glow" => Ok(Self::Glow),
            "vulkan" => Ok(Self::Vulkan),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported renderer '{value}'; expected dx12, glow, or vulkan"),
            )),
        }
    }
}

#[derive(Debug)]
pub enum Invocation {
    Launcher,
    Renderer {
        mode: RendererMode,
        ready_file: Option<PathBuf>,
    },
}

pub fn parse_invocation() -> io::Result<Invocation> {
    let mut arguments = env::args_os().skip(1);
    let mut renderer = None;
    let mut ready_file = None;

    while let Some(argument) = arguments.next() {
        if argument == RENDERER_ARGUMENT {
            if renderer.is_some() {
                return Err(invalid_argument("--renderer was specified more than once"));
            }

            let value = required_value(&mut arguments, RENDERER_ARGUMENT)?;
            let value = value
                .to_str()
                .ok_or_else(|| invalid_argument("--renderer value must be valid Unicode text"))?;
            renderer = Some(value.parse()?);
        } else if argument == READY_FILE_ARGUMENT {
            if ready_file.is_some() {
                return Err(invalid_argument(
                    "--renderer-ready-file was specified more than once",
                ));
            }

            ready_file = Some(PathBuf::from(required_value(
                &mut arguments,
                READY_FILE_ARGUMENT,
            )?));
        } else {
            return Err(invalid_argument(format!(
                "unknown argument '{}'",
                argument.to_string_lossy()
            )));
        }
    }

    match renderer {
        Some(mode) => Ok(Invocation::Renderer { mode, ready_file }),
        None if ready_file.is_some() => Err(invalid_argument(
            "--renderer-ready-file requires --renderer",
        )),
        None => Ok(Invocation::Launcher),
    }
}

pub fn run_launcher() -> io::Result<()> {
    let executable = env::current_exe()?;
    let mut failures = Vec::new();

    for mode in WINDOWS_RENDERER_PRIORITY {
        eprintln!("starting P2P Chat with the {mode} renderer");
        let outcome = spawn_renderer(&executable, mode)?;

        if outcome.status.success() {
            return Ok(());
        }

        let phase = if outcome.reached_ready_state {
            "after GUI startup"
        } else {
            "during renderer startup"
        };
        let failure = format!(
            "{mode} child exited {phase} with {}",
            describe_exit_status(outcome.status)
        );
        eprintln!("{failure}; trying the next renderer");
        failures.push(failure);
    }

    Err(io::Error::other(format!(
        "all renderer processes failed: {}",
        failures.join("; ")
    )))
}

pub fn schedule_ready_signal(ready_file: Option<PathBuf>) {
    let Some(ready_file) = ready_file else {
        return;
    };

    thread::spawn(move || {
        thread::sleep(STARTUP_GRACE_PERIOD);

        let result = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&ready_file)
            .and_then(|mut file| file.write_all(b"ready\n"));

        if let Err(error) = result {
            eprintln!(
                "failed to report renderer readiness at '{}': {error}",
                ready_file.display()
            );
        }
    });
}

struct RendererOutcome {
    status: ExitStatus,
    reached_ready_state: bool,
}

fn spawn_renderer(executable: &Path, mode: RendererMode) -> io::Result<RendererOutcome> {
    let ready_file = renderer_ready_file(mode);
    let mut command = Command::new(executable);
    command
        .arg(RENDERER_ARGUMENT)
        .arg(mode.as_str())
        .arg(READY_FILE_ARGUMENT)
        .arg(&ready_file);

    if let Some(backend) = mode.wgpu_backend() {
        command.env("WGPU_BACKEND", backend);
    } else {
        command.env_remove("WGPU_BACKEND");
    }

    let status_result = command.status();
    let reached_ready_state = ready_file.is_file();
    if let Err(error) = fs::remove_file(&ready_file)
        && error.kind() != io::ErrorKind::NotFound
    {
        eprintln!(
            "failed to remove renderer readiness file '{}': {error}",
            ready_file.display()
        );
    }

    status_result.map(|status| RendererOutcome {
        status,
        reached_ready_state,
    })
}

fn renderer_ready_file(mode: RendererMode) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    env::temp_dir().join(format!(
        "p2p-chat-renderer-{}-{mode}-{timestamp}.ready",
        process::id()
    ))
}

fn describe_exit_status(status: ExitStatus) -> String {
    match status.code() {
        Some(code) => format!("code {code} (0x{:08X})", code as u32),
        None => "no exit code".to_owned(),
    }
}

fn required_value(
    arguments: &mut impl Iterator<Item = OsString>,
    argument: &str,
) -> io::Result<OsString> {
    arguments
        .next()
        .ok_or_else(|| invalid_argument(format!("{argument} requires a value")))
}

fn invalid_argument(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_renderer_modes() {
        assert_eq!(
            "dx12".parse::<RendererMode>().expect("dx12 should parse"),
            RendererMode::Dx12
        );
        assert_eq!(
            "GLOW".parse::<RendererMode>().expect("glow should parse"),
            RendererMode::Glow
        );
        assert_eq!(
            "vulkan"
                .parse::<RendererMode>()
                .expect("vulkan should parse"),
            RendererMode::Vulkan
        );
    }

    #[test]
    fn rejects_unknown_renderer_mode() {
        assert!("metal".parse::<RendererMode>().is_err());
    }
}
