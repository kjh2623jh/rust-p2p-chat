use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let stream = TcpStream::connect("127.0.0.1:9000").await?;

    println!("Connected to signaling server");

    let (reader, mut writer) = stream.into_split();

    let mut reader = BufReader::new(reader);

    tokio::spawn(async move {
        loop {
            let mut message = String::new();

            match reader.read_line(&mut message).await {
                Ok(0) => {
                    println!("Server disconnected");
                    break;
                }

                Ok(_) => {
                    println!("[SERVER] {}", message.trim());
                }

                Err(error) => {
                    eprintln!("Read error: {error}");
                    break;
                }
            }
        }
    });

    writer.write_all(b"JOIN ABC123\n").await?;

    let stdin = io::stdin();

    let mut stdin = BufReader::new(stdin);

    loop {
        let mut input = String::new();

        stdin.read_line(&mut input).await?;

        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        let message = format!("MSG {input}\n");

        writer.write_all(message.as_bytes()).await?;
    }
}
