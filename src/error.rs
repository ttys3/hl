// third-party imports
use ansi_term::Colour;
use error_chain::error_chain;

error_chain! {
    foreign_links {
        Io(::std::io::Error);
        ParseIntError(::std::num::ParseIntError);
        Capnp(::capnp::Error);
        Bincode(::bincode::Error);
        Boxed(::std::boxed::Box<dyn std::error::Error + std::marker::Send>);
    }

    errors {
        FileNotFound(filename: String) {
            description("file not found"),
            display("file '{}' not found", HILITE.paint(filename))
        }
    }
}

pub const HILITE: Colour = Colour::Yellow;
