// Author: Nicholas Ferraz de Oliveira

use clap::ArgAction;
use clap::Parser;
use std::env;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::thread;
use std::thread::available_parallelism;
extern crate rayon;
use std::sync::{Arc, Mutex};

const BUFFER_SIZE: usize = 512 * 1024;

#[derive(Parser)]
#[command(name = "RWC")]
#[command(author = "Nicholas Ferraz <nichoferraz@gmail.com>")]
#[command(version = "1.0")]
#[command(about = "WC utility written in Rust.", long_about = None)]
struct Args {
    #[clap(short='c', long, action=ArgAction::SetTrue)]
    /// Get the file size.
    bytes: bool,

    #[clap(short='l', long, action=ArgAction::SetTrue)]
    /// Get the number of lines.
    lines: bool,
    /// Full path to the file.
    path: PathBuf,
}

fn get_file_size(path: &PathBuf) -> f64 {
    fs::metadata(path).unwrap().len() as f64
}

fn format_byte_size(bytes: f64) -> String {
    if bytes < 2_f64.powf(10.0) {
        return format!("{} Bytes", bytes.to_string());
    }
    if bytes < 2_f64.powf(20.0) {
        let bytes: f64 = bytes / 2_f64.powf(10.0);
        return format!("{:.2} KiB", bytes);
    } else if bytes < 2_f64.powf(30.0) {
        let bytes: f64 = bytes / 2_f64.powf(20.0);
        return format!("{:.2} MiB", bytes);
    } else if bytes < 2_f64.powf(40.0) {
        let bytes: f64 = bytes / 2_f64.powf(30.0);
        return format!("{:.2} GiB", bytes);
    } else if bytes < 2_f64.powf(50.0) {
        let bytes: f64 = bytes / 2_f64.powf(40.0);
        return format!("{:.2} TiB", bytes);
    } else if bytes < 2_f64.powf(60.0) {
        let bytes: f64 = bytes / 2_f64.powf(50.0);
        return format!("{:.2} PiB", bytes);
    } else if bytes < 2_f64.powf(70.0) {
        let bytes: f64 = bytes / 2_f64.powf(60.0);
        return format!("{:.2} EiB", bytes);
    } else if bytes < 2_f64.powf(80.0) {
        let bytes: f64 = bytes / 2_f64.powf(70.0);
        return format!("{:.2} ZiB", bytes);
    } else {
        let bytes: f64 = bytes / 2_f64.powf(80.0);
        return format!("{:.2} YiB", bytes);
    }
}

fn par_get_number_of_lines(path: &PathBuf) -> Result<(), io::Error> {
    let input: File = File::open(&path)?;
    let file_size: usize = get_file_size(&path) as usize;
    let cores: u64 = available_parallelism().unwrap().get() as u64;
    let buffered: BufReader<File> = BufReader::with_capacity( BUFFER_SIZE, input);
    let buff_capacity: usize = buffered.capacity();
    let buffer_segments: u64 = (file_size).div_ceil(buff_capacity) as u64;
    let segments_per_core: u64 = buffer_segments / cores;
    let mut remaining_bytes: u64 = buffer_segments % cores;
    let mut assignments: Vec<(u64, u64, u64)> = vec![];
    let mut cursor_location = 0;
    println!("Remaining bytes before assignment: {}", remaining_bytes);
    for core in 0..cores {
        let mut end = cursor_location + (segments_per_core * buff_capacity as u64);
        if remaining_bytes > 0 {
            end = cursor_location + ((segments_per_core + 1) * buff_capacity as u64);
            remaining_bytes -= 1;
        } else if core == cores {
            end = get_file_size(&path) as u64;
        }
        let assignment = (cursor_location, end, 0);
        assignments.push(assignment);
        cursor_location = end + 1;
    }
    println!("Remaining bytes: {}", remaining_bytes);
    let total_lines: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let path = path;
    let mut handle_vec: Vec<thread::JoinHandle<()>> = vec![]; // JoinHandles will go in here
    for assign in assignments {
        let total_lines_clone = Arc::clone(&total_lines);
        let path_clone = path.clone();
        let handle = std::thread::spawn(move || {
            *total_lines_clone.lock().unwrap() += read_part_of_file(path_clone, assign.0, assign.1);
        });
        handle_vec.push(handle);
    }
    handle_vec
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
    let total_lines = *total_lines.lock().unwrap() as u64;
    println!("Total lines {:?}", total_lines);
    Ok(())
}

fn read_part_of_file(path: PathBuf, start: u64, end: u64) -> u64 {
    let input: File = File::open(&path).unwrap();
    let mut buffered: BufReader<File> = BufReader::with_capacity( BUFFER_SIZE, input);
    buffered.seek(SeekFrom::Start(start as u64)).unwrap();
    let buf_size: u64 = (end - start) as u64;
    let reader: io::Take<BufReader<File>> = buffered.take(buf_size);
    let lines = reader
        .bytes()
        .filter(|b| Some(b.as_ref().unwrap()) == Some(&('\n' as u8)))
        .count();
    println!(
        "Partial file starts: {}, ends:{}, has: {} lines",
        start, end, lines
    );
    lines as u64
}

fn main() {
    env::set_var("RUST_BACKTRACE", "full");
    let args: Args = Args::parse();
    let path: std::path::PathBuf = args.path;
    let filename: &str = path.file_name().unwrap().to_str().unwrap();
    if fs::metadata(&path).is_err() {
        let _ = Err::<u64, i64>(0);
    };
    if args.bytes {
        let file_size: String = format_byte_size(get_file_size(&path));
        println!("{} size is {}.", &filename, file_size);
    } else if args.lines {
        let _ = par_get_number_of_lines(&path);
    } else {
        return;
    }
}
