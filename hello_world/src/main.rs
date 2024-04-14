use rand::Rng; // random number lib
use std::cmp::Ordering;
use std::io; // enum lib for less, greater, and equal

fn main() {
    println!("Guess the number!");
    let mut secret_number = 0;

    loop {
        let secret_number = rand::thread_rng().gen_range(1..=100); // generate a random numer
        let mut guess = String::new(); // create an mutable string variable

        println!("Please input your guess.");

        io::stdin()
            .read_line(&mut guess)
            .expect("Failed to read line");

        println!("You guessed: {guess}");
        println!("The secret number is: {secret_number}");

        let guess: u32 = match guess.trim().parse() {
            Ok(num) => num,
            Err(_) => continue,
        }; // conver the input to a number

        match guess.cmp(&secret_number) {
            Ordering::Less => println!("Too small!"),
            Ordering::Greater => println!("Too big!"),
            Ordering::Equal => println!("You win!"),
        }
    }
}
