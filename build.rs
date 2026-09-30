fn main() {
    println!("cargo:rerun-if-changed=assets/app-icon.ico");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        winresource::WindowsResource::new()
            .set_icon("assets/app-icon.ico")
            .set("ProductName", "P2P Chat")
            .set("FileDescription", "P2P Chat desktop client")
            .compile()
            .expect("failed to compile Windows application resources");
    }
}
