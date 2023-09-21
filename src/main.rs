use chrono::offset::TimeZone;
use chrono::prelude::*;
use chrono::Duration;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::env::args;
use std::fs::File;
use std::io::BufReader;
use std::io::{Read, Result, Write};
use std::iter::zip;
use std::process::exit;
use std::thread;
use std::thread::JoinHandle;

#[derive(Debug, Default)]
struct BpmData {
    x: Vec<i32>,
    y: Vec<i32>,
}

fn get_fs(ring: &str) -> Result<f64> {
    const HOST: &str = "fa";
    let port: u16 = match ring.to_lowercase().as_str() {
        "r1" => 12001,
        "r3" => 32001,
        _ => {
            eprintln!("Could not understand the `--ring` flag; `{ring}`");
            exit(1);
        }
    };
    let cmd = "CF\n";

    let mut buf = Vec::new();

    let mut stream = std::net::TcpStream::connect((HOST, port))?;

    stream.write_all(cmd.as_bytes())?;
    stream.read_to_end(&mut buf)?;

    let info = std::str::from_utf8(&buf).unwrap();

    let infvec: Vec<&str> = info.split('\n').collect();
    let fs = infvec[0].parse::<f64>().unwrap();

    Ok(fs)
}

fn get_archived_data(ring: &str, start_string: &str, end_string: &str) -> Result<Vec<BpmData>> {
    println!(
        "{}: entered get_archived_data function",
        Local::now().timestamp_millis()
    );
    const HOST: &str = "fa";
    let port: u16;
    let bpm_range: Vec<String>;
    let bpm_cmd_str: String;

    let start_dt = match Local.datetime_from_str(&start_string, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(dt) => dt,
        _ => {
            eprintln!("Could not understand the `--start` flag; `{start_string}`");
            exit(1);
        }
    };
    let end_dt = match Local.datetime_from_str(&end_string, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(dt) => dt,
        _ => {
            eprintln!("Could not understand the `--end` flag; `{end_string}`");
            exit(1);
        }
    };
    let start_seconds = start_dt.timestamp();
    let start_nanos = start_dt.timestamp_nanos() - start_seconds * 1_000_000_000;
    let end_seconds = end_dt.timestamp();
    let end_nanos = end_dt.timestamp_nanos() - end_seconds * 1_000_000_000;

    match ring.to_lowercase().as_str() {
        "r1" => {
            port = 12001;
            bpm_range = (1..37).map(|x| x.to_string()).collect();
            bpm_cmd_str = "1-36".to_string();
        }
        "r3" => {
            port = 32001;
            bpm_range = (1..201).map(|x| x.to_string()).collect();
            bpm_cmd_str = "1-200".to_string();
        }
        _ => panic!("Unknown ring: {}", ring),
    };
    const CHKBYTESIZE: usize = 1;
    const HDRSIZE: usize = 8;
    const DATSIZE: usize = 4;

    let mut datasets = vec![];

    let cmd_str = format!(
        "RFM{}S{}.{:09}ES{}.{:09}N\n",
        bpm_cmd_str, start_seconds, start_nanos, end_seconds, end_nanos,
    );
    println!(
        "{}: Sending the command: '{}'",
        Local::now().timestamp_millis(),
        cmd_str.trim()
    );

    let mut checkbyte = [0u8; CHKBYTESIZE];
    let mut header = [0u8; HDRSIZE];
    // let mut buf = Vec::new();
    let total_time = (end_dt.timestamp_nanos() - start_dt.timestamp_nanos()) / 1_000_000_000;
    let mut buf = Vec::with_capacity(16222400 * total_time as usize);

    let mut stream = std::net::TcpStream::connect((HOST, port))?;

    stream.write_all(cmd_str.as_bytes())?;
    println!(
        "{}: Reading data from stream",
        Local::now().timestamp_millis()
    );
    stream.read_exact(&mut checkbyte)?;
    stream.read_exact(&mut header)?;
    let mut reader = BufReader::new(&stream);
    let read_bytes = reader.read_to_end(&mut buf)?;
    // let read_bytes = stream.read_to_end(&mut buf)?;
    println!(
        "{}: Read {} bytes",
        Local::now().timestamp_millis(),
        read_bytes
    );

    println!("{}: Parsing data", Local::now().timestamp_millis());
    let mut values = Vec::new();
    for i in (0..buf.len()).step_by(DATSIZE) {
        let val = i32::from_ne_bytes((&buf[i..i + DATSIZE]).try_into().unwrap());
        values.push(val);
    }

    for (i, _) in bpm_range.iter().enumerate() {
        let d = BpmData {
            x: values[2 * i..]
                .iter()
                .step_by(bpm_range.len() * 2)
                .cloned()
                .collect::<Vec<i32>>(),
            y: values[2 * i + 1..]
                .iter()
                .step_by(bpm_range.len() * 2)
                .cloned()
                .collect::<Vec<i32>>(),
        };
        datasets.push(d);
    }

    println!("{}: Returning parsed data", Local::now().timestamp_millis());
    Ok(datasets)
}

fn print_help(exe_name: &str) {
    eprintln!("{exe_name} --ring R1|R3 --start YYYY-MM-DDThh:mm:ss.s --end YYYY-MM-DDThh:mm:ss.s");
}

fn main() {
    let mut args: VecDeque<String> = args().collect();
    let exe_name = args.pop_front().unwrap();
    if args.len() != 6 {
        println!("\nCould not find the correct number of arguments.");
        print_help(&exe_name);
        exit(1);
    }

    let mut start_time: String = "".to_string();
    let mut end_time: String = "".to_string();
    let mut ring: String = "".to_string();

    while !args.is_empty() {
        let next_arg = args.pop_front().unwrap();
        match next_arg.as_str() {
            "-h" | "--help" => print_help(&exe_name),
            "--start" => match args.pop_front() {
                Some(expr) => start_time = expr,
                None => unreachable!("Code should never get here"),
            },
            "--end" => match args.pop_front() {
                Some(expr) => end_time = expr,
                None => unreachable!("Code should never get here"),
            },
            "--ring" => match args.pop_front() {
                Some(expr) => ring = expr,
                None => unreachable!("Code should never get here"),
            },
            e => {
                eprintln!("{e}");
                exit(1);
            }
        }
    }

    let start_dt = match Local.datetime_from_str(&start_time, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(dt) => dt,
        _ => {
            eprintln!("Could not understand the `--start` flag; `{start_time}`");
            exit(1);
        }
    };

    let fs = match get_fs(&ring) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("{e}");
            exit(1);
        }
    };
    let timestep_nanoseconds: f64 = 1_000_000_000f64 / fs;

    let data = match get_archived_data(&ring, &start_time, &end_time) {
        Ok(reply) => reply,
        _ => {
            eprintln!("There was a problem getting data from the archiver.");
            eprintln!("Are you within the MAXIV firewall?");
            exit(1);
        }
    };

    println!("{}: Writing to file.", Local::now().timestamp_millis());
    let mut filenum = 0;
    let mut thread_list = Vec::<JoinHandle<()>>::new();
    for bpm in data {
        thread_list.push(thread::spawn(move || {
            write_bpmdata_to_file(filenum.clone(), bpm, timestep_nanoseconds.clone(), start_dt);
        }));
        filenum += 1;
    }
    println!(
        "{}: Waiting for file-write threads to finish. This can take some time for large datasets.",
        Local::now().timestamp_millis()
    );
    for thr in thread_list {
        let _ = thr.join().unwrap();
    }
    println!("{}: Done!", Local::now().timestamp_millis());
}

fn write_bpmdata_to_file(
    filenum: usize,
    bpm: BpmData,
    timestep_nanoseconds: f64,
    start_dt: DateTime<Local>,
) {
    let fname = format!("bpm_{:03}.dat", filenum);
    let mut file = File::create(fname).unwrap();
    write!(file, "# FA data for BPM #{:03}\n", filenum).unwrap();
    write!(file, "# t, x, y\n").unwrap();

    let mut timestep = 0;
    for (x, y) in zip(&bpm.x, &bpm.y) {
        let timestamp =
            start_dt + Duration::nanoseconds((timestep as f64 * timestep_nanoseconds) as i64);
        write!(
            file,
            "{}, {}, {}\n",
            timestamp.format("%Y-%m-%d_%H:%M:%S.%f"),
            x,
            y
        )
        .unwrap();
        timestep += 1;
    }
}
