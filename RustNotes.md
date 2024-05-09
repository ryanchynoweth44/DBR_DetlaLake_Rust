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


Types safety and immutability in rust is extremely beneficial for preventing bugs. Especially if you begin to think about code bases shared by larger teams in which individuals may be operating on the same set of classes and may attempt to name variable names the same. **Constants** in rust are always immutable and declared using the `const` keyword. Constants can be declared anywhere in the code, including global variables which can be helpful for things like environment vars. Constants will have ALL_UPPER_CASE_LETTERS_WITH_UNDERSCORES as the names. Constants are essentially hard coded values. 

Rust allows you to shadow variables within a scope that is limited. this is different than mutable variables as we use the `let` keyword to actually create a new variable in that scope. Shadowing also allows us to change the type of the variable on the fly as well. For example the following code and output: 
```
fn main() {
    let x = 5;

    let x = x + 1;

    {
        let x = x * 2;
        println!("The value of x in the inner scope is: {x}");
    }

    println!("The value of x is: {x}");
}
```
Output: 
```
The value of x in the inner scope is: 12
The value of x is: 6
```


### Data Types 

Rust is statically typed. 

A scalar type represents a single value. Rust has four primary scalar types: integers, floating-point numbers, Booleans, and characters. 
- signed integers start with `i` and unsigned start with `u`. 
- 8 bit to 128 bit and have a `size` which depends on the computer architecture 
- signed integers can be negative whil unsigned are always positive.  
- Floating points are f32 and f64. 
- `char` types are denoted with single quotes and strings are double quotes. 

A compound type can group multiple values into a single type. 
- Tuples: fixed length once declared and look like the following: `(1,2,3,...,n)`
    - denoted with `tup` keyword. 
- Arrays: 
    - Every element of the array must be the same type 
    - Have a fixed length and denoted with square brackets `[]`. 
    - Arrays are useful when you want your data allocated on the stack rather than the heap. 
    - Not as flexible as vector data types. 

### Functions 
- [Resource](https://doc.rust-lang.org/book/ch03-03-how-functions-work.html)

Rust uses `fn` to define functions with `main` being the entrypoint. Functions are defined using `snake_case` for syntax. 

The order of which functions are defined does not matter because its a compiled language. This is different from a language like Python. 

In function signatures, you must declare the type of each parameter. This is a deliberate decision in Rust’s design: requiring type annotations in function definitions means the compiler almost never needs you to use them elsewhere in the code to figure out what type you mean. The compiler is also able to give more helpful error messages if it knows what types the function expects. Functions are defined with the following structure: `fn function_name(parameter_one: type_one, parameter_two: type_two) -> return_type {...}`. 

- Statements are instructions that perform some action and do not return a value.
- Expressions evaluate to a resultant value. Let’s look at some examples.
    - Expressions do not include ending semicolons. If you add a semicolon to the end of an expression, you turn it into a statement, and it will then not return a value. Keep this in mind as you explore function return values and expressions next.

Functions will automatically return the final expression value but can be returned early using the `return` keyword. Often the return keyword is not used. 

### Control Flow

If statements follow java like syntax with `if`, `else if`, and `else`. However, they do not require the parenthesis in the evaluation clause. See below for an example. 
```
fn main() {
    let number = 6;

    if number % 4 == 0 {
        println!("number is divisible by 4");
    } else if number % 3 == 0 {
        println!("number is divisible by 3");
    } else if number % 2 == 0 {
        println!("number is divisible by 2");
    } else {
        println!("number is not divisible by 4, 3, or 2");
    }
}
```

Single line if statements: `let number = if condition { 5 } else { 6 };` 

#### Loops 

Rust has three kinds of loops: `loop`, `while`, and `for`. Let’s try each one.
- Loop will repeat the clause until it is explicitly broken. 
    - Loops can be labeled to explicitly handle nested/many loops. `'counting_up: loop`
- While will go until condition is met
- For will go until iterator is out of items. 


### Ownership 
Ownership is a set of rules that govern how a Rust program manages memory. All programs have to manage the way they use a computer’s memory while running. Some languages have garbage collection that regularly looks for no-longer-used memory as the program runs; in other languages, the programmer must explicitly allocate and free the memory. Rust uses a third approach: memory is managed through a system of ownership with a set of rules that the compiler checks. If any of the rules are violated, the program won’t compile. None of the features of ownership will slow down your program while it’s running.

[Documentation Resource](https://doc.rust-lang.org/book/ch04-01-what-is-ownership.html)

Stack vs Heap Memory: 
- The stack stores values in the order it gets them and removes the values in the opposite order. Last in first out. 
- heap memory requires requesting a certain amount of space. The memory allocator finds an empty spot in the heap that is big enough, marks it as being in use, and returns a pointer, which is the address of that location. 
- Stack is faster than heap 


Rust takes a different path: the memory is automatically returned once the variable that owns it goes out of scope. 

For variables rust makes shallow clones when you set one variable equal to another. Deep clones can be explicitly called as needed. 

The `&s1` syntax lets us create a reference that refers to the value of s1 but does not own it. Because it does not own it, the value it points to will not be dropped when the reference stops being used. The `&` allows us to essentially enforces the fact that we cannot alter the variable. For example, if I pass the variable into a function and try to change it then it will error out. Inversely if I do not provide the `&` then it is able to be changed. Note that it is possible to set parameters to a function to be mutable. 


String is a mutable heap memory objet. While str is a data type that is a subset of string and is immutable. This allows engineers to conserve memory by pointing to the same address in memory without duplicating objects. 


https://doc.rust-lang.org/book/ch05-00-structs.html