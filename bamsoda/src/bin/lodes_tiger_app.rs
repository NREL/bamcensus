use bamsoda::app::lodes_tiger_args::LodesTigerArgs;
use clap::Parser;

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = LodesTigerArgs::parse();
    cli.run().await
}
