use std::env;

fn main() {
    match lethe_review_harness::run_cli(env::args().skip(1)) {
        Ok(output) => {
            print!("{output}");
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
