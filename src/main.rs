use chrono::offset::TimeZone;
use chrono::prelude::*;
use chrono::Duration;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::env::args;
use std::fs::File;
use std::io::{Read, Result, Write};
use std::iter::zip;
use std::process::exit;

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
        _ => panic!("Unknown ring: {}", ring),
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
    const HOST: &str = "fa";
    let port: u16;
    let bpm_range: Vec<String>;
    let bpm_cmd_str: String;

    let start_dt = Local
        .datetime_from_str(start_string, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap();
    let end_dt = Local
        .datetime_from_str(end_string, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap();
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
    println!("Sending the command: '{cmd_str}'");

    let mut checkbyte = [0u8; CHKBYTESIZE];
    let mut header = [0u8; HDRSIZE];
    let mut buf = Vec::new();

    let mut stream = std::net::TcpStream::connect((HOST, port))?;

    stream.write_all(cmd_str.as_bytes())?;
    stream.read_exact(&mut checkbyte)?;
    stream.read_exact(&mut header)?;
    stream.read_to_end(&mut buf)?;

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
                None => todo!(),
            },
            "--end" => match args.pop_front() {
                Some(expr) => end_time = expr,
                None => todo!(),
            },
            "--ring" => match args.pop_front() {
                Some(expr) => ring = expr.to_lowercase(),
                None => todo!(),
            },
            e => {
                eprintln!("{e}");
                exit(1);
            }
        }
    }

    let fs = match get_fs(&ring) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("{e}");
            exit(1);
        }
    };
    let timestep_nanoseconds: f64 = 1_000_000_000f64 / fs;
    let start_dt = Local
        .datetime_from_str(&start_time, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap();
    println!("timestep_nanoseconds = {timestep_nanoseconds}");
    println!("start_dt = {start_dt}");
    println!(
        "start_dt + timestep_nanoseconds = {}",
        start_dt + Duration::nanoseconds(timestep_nanoseconds as i64)
    );

    let data = match get_archived_data(&ring, &start_time, &end_time) {
        Ok(reply) => reply,
        _ => todo!(),
    };

    let mut filenum = 0;
    for bpm in data {
        let mut timestep = 0;
        let fname = format!("bpm_{:03}.dat", filenum);
        println!("Writing file: {fname}");
        let mut file = File::create(fname).unwrap();
        write!(file, "# FA data\n").unwrap();
        write!(file, "# t, x, y\n").unwrap();
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
        filenum += 1;
    }
}
