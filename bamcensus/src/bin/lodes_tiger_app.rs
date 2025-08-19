use bamcensus::app::lodes_tiger_args::LodesTigerCli;
use clap::Parser;

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = LodesTigerCli::parse();
    cli.run().await
}
