use clap::Parser;
use bamsoda_app::app::lodes_tiger_args::LodesTigerArgs;

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = LodesTigerArgs::parse();
    cli.run().await
}
