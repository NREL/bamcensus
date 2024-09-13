use clap::Parser;
use us_census_app::app::lodes_tiger_args::LodesTigerArgs;

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = LodesTigerArgs::parse();
    cli.run().await
}
