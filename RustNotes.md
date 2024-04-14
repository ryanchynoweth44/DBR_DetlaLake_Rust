# Notes 

## The Rust Programming Language - [Book](https://doc.rust-lang.org/book/)

Rust looks to provide easier programming for low level tasks with speed and memory safetfy/management. While it is great for low level tasks it is built in a way to still make it usable for higher level functions like CLIs and Web Servers. 

In Rust, the compiler plays a gatekeeper role by refusing to compile code with subtle bugs, that are normally only caught through thorough testing. This includes concurrency bugs. By working alongside the compiler, the team can spend their time focusing on the program’s logic rather than chasing down bugs.


- Cargo, the included dependency manager and build tool, makes adding, compiling, and managing dependencies painless and consistent across the Rust ecosystem.
- The Rustfmt formatting tool ensures a consistent coding style across developers.
- The Rust Language Server powers Integrated Development Environment (IDE) integration for code completion and inline error messages.

The `main` function is special: it is always the first code that runs in every executable Rust program. In the hello world example the `main` function has no parameters and returns nothing. If there were parameters, they would go inside the parentheses (). So in a way rust can be written in functional style as needed too. The function body is wrapped in {}. Rust requires curly brackets around all function bodies. It’s good style to place the opening curly bracket on the same line as the function declaration, adding one space in between.

One thing to note in rust there is a concept of macros. The hello world example has a `println!` which indicates a macro. Macros are unqiue functions in rust. 
-  macros are a way of writing code that writes other code, which is known as metaprogramming.
- A function signature must declare the number and type of parameters the function has. Macros, on the other hand, can take a variable number of parameters. 


Just compiling with rustc is fine for simple programs, but as your project grows, you’ll want to manage all the options and make it easy to share your code.


## Cargo 

Create a cargo project with the following command that has two files. `src/main.rs` and `Cargo.toml`. 
```
cargo new dbrx_rest_api
```

The build and run the application with the following:
```
cargo build

cargo run
```

`cargo run` will also build and run the project by itself to streamline the commands. `cargo check` will also validate the project faster than a build. Lastly, `cargo build --release` will package the project with optimizations and add it to a release folder instead of a debug folder. 

## General Guidelines 

 In Rust, variables are immutable by default, meaning once we give the variable a value, the value won’t change. To make variable mutable then you must use `let mut <var>`. 

 You can import package with the `use` command. For example, the standard library with `io` is the following import `use std::io;`. 

A `match` expression is made up of arms. An arm consists of a `pattern` to `match` against, and the code that should be run if the value given to match fits that arm’s pattern. Rust takes the value given to match and looks through each arm’s pattern in turn. Patterns and the match construct are powerful Rust features: they let you express a variety of situations your code might encounter and they make sure you handle them all.